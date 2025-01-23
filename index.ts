import puppeteer, { Browser, Page } from "puppeteer";
import { readdir, readFile, writeFile, mkdir, access } from "fs/promises";
import Papa from "papaparse";
import fs from "fs";
import path from "path";
import { URL } from "url";

// Configuration constants
const CONFIG = {
  OUTPUT: "out" + ".csv",
  SEARCH_LIMIT: 50, // Maximum number of CSV links to process
  ALLOWED_COLUMN_KEYWORDS: [
    "name",
    "phone",
    "zip",
    "contact",
    "email",
    "state",
  ],
  DIRECTORIES: {
    CSV_SAVE: "./csv_files_95",
    LINKS_FILE: "./links.txt",
  },
  TIMEOUTS: {
    PAGE_LOAD: 10000,
    NEXT_PAGE_DELAY: 4000,
  },
};

interface ScraperOptions {
  headless?: boolean;
  maxLinks?: number;
}

class CSVScraper {
  private browser: Browser | null = null;
  private page: Page | null = null;
  private allLinks: string[] = [];
  private oldLinks: string = "";

  constructor(private options: ScraperOptions = {}) {}

  private async ensureDirectory(dirPath: string): Promise<void> {
    try {
      await access(dirPath);
    } catch {
      console.log(`Creating directory: ${dirPath}`);
      await mkdir(dirPath, { recursive: true });
    }
  }

  private async initialize(): Promise<void> {
    console.log("Initializing browser and page...");
    this.browser = await puppeteer.launch({
      headless: this.options.headless ?? false,
    });
    this.page = await this.browser.newPage();

    // Set up more robust page navigation
    this.page.setDefaultTimeout(CONFIG.TIMEOUTS.PAGE_LOAD);
    this.page.setDefaultNavigationTimeout(CONFIG.TIMEOUTS.PAGE_LOAD);
  }

  private async loadOldLinks(): Promise<void> {
    try {
      this.oldLinks = await Bun.file(CONFIG.DIRECTORIES.LINKS_FILE).text();
      console.log(`Loaded ${this.oldLinks.split("\n").length} existing links`);
    } catch (error) {
      console.warn("No existing links file found. Starting fresh.");
      this.oldLinks = "";
    }
  }

  private async handleCookieConsent(): Promise<void> {
    try {
      const cookieButton = await this.page!.$(
        'button[aria-label="Accept all"]',
      );
      if (cookieButton) {
        console.log("Accepting cookie consent");
        await cookieButton.click();
      }
    } catch (error) {
      console.warn("Could not handle cookie consent:", error);
    }
  }

  private async performSearch(searchTerm: string): Promise<void> {
    console.log(`Searching for: "${searchTerm}"`);
    await this.page!.goto("https://www.google.com", {
      waitUntil: "domcontentloaded",
    });

    await this.handleCookieConsent();

    await this.page!.locator("#APjFqb").fill(searchTerm);
    await this.page!.keyboard.press("Enter");
    await this.page!.waitForSelector("#search");
  }

  private async extractCsvLinks(): Promise<string[]> {
    return await this.page!.evaluate(() => {
      return Array.from(document.querySelectorAll("a"))
        .map((a) => a.href)
        .filter((href) => href.endsWith(".csv"));
    });
  }

  private async collectCsvLinks(searchTerm: string): Promise<void> {
    await this.performSearch(searchTerm);

    const maxLinks = this.options.maxLinks ?? CONFIG.SEARCH_LIMIT;
    console.log(`Collecting up to ${maxLinks} CSV links`);

    while (this.allLinks.length < maxLinks) {
      const links = await this.extractCsvLinks();

      const newLinks = links.filter((link) => !this.oldLinks.includes(link));
      this.allLinks = [...this.allLinks, ...newLinks];

      console.log(
        `Found ${newLinks.length} new CSV links. Total: ${this.allLinks.length}`,
      );

      if (this.allLinks.length >= maxLinks) break;

      // Try to navigate to next page
      const nextButton = await this.page!.$("#pnnext");
      if (!nextButton) {
        console.log("No more search pages available");
        break;
      }

      await nextButton.click();
      await delay(CONFIG.TIMEOUTS.NEXT_PAGE_DELAY);
    }
  }

  private async downloadAndProcessCsvs(): Promise<void> {
    await this.ensureDirectory(CONFIG.DIRECTORIES.CSV_SAVE);

    for (const link of this.allLinks) {
      try {
        console.log(`Processing CSV link: ${link}`);
        const response = await fetch(link);

        if (!response.ok) {
          console.error(`Failed to download CSV: ${link}`);
          continue;
        }

        const csvContent = await response.text();
        const fileName = this.extractFileName(link);
        const filePath = path.join(
          CONFIG.DIRECTORIES.CSV_SAVE,
          fileName.replaceAll(" ", "-").replaceAll("%20", "-"),
        );
        await this.processCsvContent(csvContent, filePath);
        const proc = Bun.spawn({
          cmd: ["python3", "utils/split.py", filePath, filePath],
          stdout: "pipe",
          stderr: "pipe",
        });

        const { stdout, stderr } = await proc;

        // Function to read a ReadableStream to a string
        async function streamToString(stream) {
          const reader = stream.getReader();
          let decoder = new TextDecoder();
          let result = "";
          let done = false;

          while (!done) {
            const { value, done: readerDone } = await reader.read();
            result += decoder.decode(value, { stream: true });
            done = readerDone;
          }

          result += decoder.decode(); // Finalize decoding
          return result;
        }

        if (stderr) {
          const errorOutput = await streamToString(stderr);
          console.error("Error output:", errorOutput);
        }

        if (stdout) {
          const output = await streamToString(stdout);
          console.log("Python script output:", output);
        }
      } catch (error) {
        console.error(`Error processing ${link}:`, error);
      }
    }
  }

  private extractFileName(link: string): string {
    try {
      const parsedUrl = new URL(link);
      return path.basename(parsedUrl.pathname) || `download_${Date.now()}.csv`;
    } catch {
      return `download_${Date.now()}.csv`;
    }
  }

  private async processCsvContent(
    csvContent: string,
    filePath: string,
  ): Promise<void> {
    try {
      // Check for forbidden terms in the CSV content
      const forbiddenTerms = ["agent", "broker", "realtor"];
      const contentLowerCase = csvContent.toLowerCase();

      if (forbiddenTerms.some((term) => contentLowerCase.includes(term))) {
        console.log(`Skipping CSV as it mentions forbidden terms: ${filePath}`);
        return;
      }

      // Parse the CSV with columns
      const { data, meta } = Papa.parse(csvContent, {
        header: true,
        skipEmptyLines: true,
      });

      if (meta.fields) {
        // Filter columns to only keep those with ALLOWED_COLUMN_KEYWORDS
        const filteredColumns = meta.fields.filter((field) =>
          CONFIG.ALLOWED_COLUMN_KEYWORDS.some((keyword) =>
            field.toLowerCase().includes(keyword),
          ),
        );

        // If no columns match the allowed keywords, skip the file
        if (filteredColumns.length === 0) {
          console.log(`No allowed columns found in: ${filePath}. Skipping.`);
          return;
        }

        // Find phone columns
        const phoneColumns = filteredColumns.filter((field) =>
          field.toLowerCase().includes("phone"),
        );

        // If no phone columns, skip the file
        if (phoneColumns.length === 0) {
          console.log(`Skipping CSV without phone header: ${filePath}`);
          return;
        }

        // Clean and filter the data
        const cleanedData = data
          .map((row) => {
            // Create a new row with cleaned data for filtered columns
            const cleanedRow: Record<string, string> = {};

            filteredColumns.forEach((column) => {
              if (phoneColumns.includes(column) && row[column]) {
                // Clean phone number: remove non-digit characters
                const cleanedPhone = row[column].replace(/\D/g, "");
                cleanedRow[column] = cleanedPhone;
              } else {
                cleanedRow[column] = row[column] || "";
              }
            });

            return cleanedRow;
          })
          .filter((row) =>
            // Keep only rows with at least one phone column that has a valid phone number
            phoneColumns.some(
              (column) =>
                row[column] &&
                row[column].length >= 10 && // Ensure minimum phone number length
                /^\d+$/.test(row[column]), // Ensure only digits
            ),
          );

        // If no rows remain after filtering, skip saving
        if (cleanedData.length === 0) {
          console.log(`No valid phone numbers found in: ${filePath}`);
          return;
        }

        // Convert cleaned data back to CSV
        const cleanedCsv = Papa.unparse(cleanedData, { header: true });

        // Save the cleaned CSV
        fs.writeFileSync(filePath, cleanedCsv);
        console.log(`Processed and saved cleaned CSV: ${filePath}`);
        console.log(`Columns retained: ${filteredColumns.join(", ")}`);
        console.log(`Rows after cleaning: ${cleanedData.length}`);
      }
    } catch (error) {
      console.error(`CSV processing error for ${filePath}:`, error);
    }
  }

  private async saveLinks(): Promise<void> {
    await Bun.write(
      CONFIG.DIRECTORIES.LINKS_FILE,
      this.allLinks.join("\n") + "\n" + this.oldLinks,
    );
    console.log(
      `Saved ${this.allLinks.length} links to ${CONFIG.DIRECTORIES.LINKS_FILE}`,
    );
  }

  async scrape(searchTerm: string): Promise<void> {
    try {
      console.log("Starting CSV scraping process...");
      await this.initialize();
      await this.loadOldLinks();
      await this.collectCsvLinks(searchTerm);
      await this.downloadAndProcessCsvs();
      await this.saveLinks();
    } catch (error) {
      console.error("Critical scraping error:", error);
    } finally {
      if (this.browser) {
        await this.browser.close();
        console.log("Browser closed successfully");
      }
    }
  }
}

// Main execution
async function main() {
  const args = process.argv.slice(2);
  if (args.length === 0) {
    console.error("Please provide a search term.");
    process.exit(1);
  }

  const searchTerm = args.join(" ");
  const scraper = new CSVScraper();
  await scraper.scrape(searchTerm);
  const proc = Bun.spawn({
    cmd: [
      "python3",
      "utils/format.py",
      CONFIG.DIRECTORIES.CSV_SAVE,
      CONFIG.OUTPUT,
    ],
    stdout: "pipe",
    stderr: "pipe",
  });

  const { stdout, stderr } = await proc;

  // Function to read a ReadableStream to a string
  async function streamToString(stream) {
    const reader = stream.getReader();
    let decoder = new TextDecoder();
    let result = "";
    let done = false;

    while (!done) {
      const { value, done: readerDone } = await reader.read();
      result += decoder.decode(value, { stream: true });
      done = readerDone;
    }

    result += decoder.decode(); // Finalize decoding
    return result;
  }

  if (stderr) {
    const errorOutput = await streamToString(stderr);
    console.error("Error output:", errorOutput);
  }

  if (stdout) {
    const output = await streamToString(stdout);
    console.log("Python script output:", output);
  }
}

// Utility function for CSV processing
function cleanPhoneNumber(phone: string): string {
  return phone.replace(/\D/g, "");
}

// Optional: Additional CSV file processing function
async function processCsvFiles() {
  try {
    const files = await readdir(CONFIG.DIRECTORIES.CSV_SAVE);
    const csvFiles = files.filter((file) => file.endsWith(".csv"));

    for (const file of csvFiles) {
      const filePath = path.join(CONFIG.DIRECTORIES.CSV_SAVE, file);

      await processCsvFile(filePath);
    }
  } catch (error) {
    console.error("Error processing CSV files:", error);
  }
}

async function processCsvFile(filePath: string) {
  try {
    // Step 1: Run Python script to preprocess the CSV file

    const fileContent = await readFile(filePath, "utf-8");

    // Step 3: Parse CSV data using PapaParse
    const { data, meta } = Papa.parse(fileContent, {
      header: true,
      skipEmptyLines: true,
    });

    if (!meta.fields) {
      console.error(`No fields detected in ${filePath}.`);
      return;
    }

    // Step 4: Filter columns based on allowed keywords
    const filteredColumns = meta.fields.filter((field) =>
      CONFIG.ALLOWED_COLUMN_KEYWORDS.some((keyword) =>
        field.toLowerCase().includes(keyword),
      ),
    );

    if (filteredColumns.length === 0) {
      console.log(`No allowed columns found in ${filePath}. Skipping.`);
      return;
    }

    // Step 5: Map and clean data
    const filteredData = data.map((row) => {
      const filteredRow: Record<string, string> = {};
      filteredColumns.forEach((column) => {
        if (column.toLowerCase().includes("phone") && row[column]) {
          // Clean phone number column by removing non-digit characters
          filteredRow[column] = cleanPhoneNumber(row[column]);
        } else {
          filteredRow[column] = row[column] || ""; // Retain other values as-is
        }
      });
      return filteredRow;
    });

    // Step 6: Convert filtered data back to CSV format
    const filteredCsv = Papa.unparse(filteredData, { header: true });

    // Step 7: Save the filtered CSV back to the file
    await writeFile(filePath, filteredCsv);

    console.log(
      `Processed file: ${filePath}, Columns retained: ${filteredColumns.join(", ")}`,
    );
  } catch (error) {
    console.error(`Error processing file ${filePath}:`, error);
  }
}
function delay(time) {
  return new Promise(function (resolve) {
    setTimeout(resolve, time);
  });
}
main().catch(console.error);

export { CSVScraper, processCsvFiles, cleanPhoneNumber };
