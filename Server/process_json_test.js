import https from 'https';
import http from 'http';
import fs from 'fs';
 
// Configuration
const OUTPUT_FILE = 'output.json';
 
// Validation helpers
const isValidId = (id) => {
    if (!id || typeof id !== 'string') return false;
    if (id === 'BAD-ID' || id === 'null' || id.trim() === '') return false;
    return true;
};
 
const isValidPrice = (price) => {
    if (typeof price === 'string' && price.trim() === 'N/A') return false;
    const num = parseFloat(price);
    return !isNaN(num) && num >= 0;
};
 
const isValidQuantity = (qty) => {
    return typeof qty === 'number' && !isNaN(qty);
};
 
const cleanName = (name) => {
    if (!name || typeof name !== 'string') return null;
    // Strip leading/trailing whitespace
    let cleaned = name.trim();
    // Strip leading question marks
    cleaned = cleaned.replace(/^\?+/, '').trim();
    // Strip HTML tags but preserve their content
    cleaned = cleaned.replace(/<\/?[^>]+(>|$)/g, '');
    // Trim again after tag removal
    cleaned = cleaned.trim();
    return cleaned.length > 0 ? cleaned : null;
};
 
const normalizeCategory = (cat) => {
    if (!cat || cat === 'unknown') return 'uncategorized';
    return cat;
};
 
// Attempt to parse potentially truncated JSON by salvaging complete objects
const parsePossiblyTruncated = (rawData) => {
    // First, try a normal parse
    try {
        return JSON.parse(rawData);
    } catch (e) {
        console.warn('Standard JSON parse failed, attempting recovery of truncated JSON...');
    }
 
    // Try to extract complete JSON objects from a truncated array
    // Strategy: find each top-level object by matching braces
    const records = [];
    let depth = 0;
    let start = -1;
    let inString = false;
    let escape = false;
 
    for (let i = 0; i < rawData.length; i++) {
        const ch = rawData[i];
 
        if (escape) {
            escape = false;
            continue;
        }
 
        if (ch === '\\' && inString) {
            escape = true;
            continue;
        }
 
        if (ch === '"') {
            inString = !inString;
            continue;
        }
 
        if (inString) continue;
 
        if (ch === '{') {
            if (depth === 0) start = i;
            depth++;
        } else if (ch === '}') {
            depth--;
            if (depth === 0 && start !== -1) {
                const objStr = rawData.slice(start, i + 1);
                try {
                    const obj = JSON.parse(objStr);
                    records.push(obj);
                } catch (err) {
                    // skip malformed object
                }
                start = -1;
            }
        }
    }
 
    if (records.length > 0) {
        console.warn(`Recovered ${records.length} complete record(s) from truncated JSON.`);
        return records;
    }
 
    throw new Error('Could not parse or recover any records from the JSON data.');
};
 
// Fetch data from remote URL (supports http and https)
const fetchInventory = (url) => {
    debugger;
    return new Promise((resolve, reject) => {
        const client = url.startsWith('https') ? https : http;
        client.get(url, (res) => {
            if (res.statusCode !== 200) {
                reject(new Error(`Failed to fetch: ${res.statusCode}`));
                return;
            }
 
            const chunks = [];
            res.on('data', (chunk) => chunks.push(chunk));
            res.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
            res.on('error', reject);
        }).on('error', reject);
    });
};
 
// Process inventory records
function processInventory(records) {
    // Map of sku -> best (most recent) record
    const skuMap = new Map();
 
    let totalProcessed = 0;
    let invalidSkipped = 0;
 
    for (const record of records) {
        totalProcessed++;
 
        // --- Record Skipping ---
        if (!isValidId(record.id)) {
            invalidSkipped++;
            continue;
        }
 
        if (!isValidPrice(record.price)) {
            invalidSkipped++;
            continue;
        }
 
        if (!isValidQuantity(record.quantity)) {
            invalidSkipped++;
            continue;
        }
 
        // --- Data Cleaning ---
        const cleanedName = cleanName(record.name);
        // (name cleaning failure doesn't skip — we just store null and let it pass through)
 
        const cleanedRecord = {
            id: record.id,
            sku: record.sku,
            name: cleanedName,
            price: record.price,
            quantity: record.quantity,
            category: normalizeCategory(record.category),
            last_updated: record.last_updated
            // Extra fields are intentionally omitted
        };
 
        // --- Deduplication: keep most recent by last_updated ---
        const sku = record.sku;
        if (skuMap.has(sku)) {
            const existing = skuMap.get(sku);
            const existingDate = new Date(existing.last_updated);
            const newDate = new Date(cleanedRecord.last_updated);
            if (newDate > existingDate) {
                skuMap.set(sku, cleanedRecord);
            }
        } else {
            skuMap.set(sku, cleanedRecord);
        }
    }
 
    const validRecords = Array.from(skuMap.values());
    const duplicatesHandled = totalProcessed - invalidSkipped - validRecords.length;
 
    return {
        validRecords,
        stats: {
            total: totalProcessed,
            invalid: invalidSkipped,
            duplicates: duplicatesHandled < 0 ? 0 : duplicatesHandled,
            clean: validRecords.length
        }
    };
}
 
// Main ETL process
async function runETL() {
    const remoteUrl = process.argv[2];
    if (!remoteUrl) {
        console.error('Usage: node process_json_test.js <JSON_FILE_URL>');
        process.exit(1);
    }
 
    console.log('Starting ETL process...');
    console.log(`Fetching data from: ${remoteUrl}`);
 
    try {
        const rawData = await fetchInventory(remoteUrl);
        console.log(`Downloaded ${rawData.length} bytes`);
 
        const records = parsePossiblyTruncated(rawData);
        console.log(`Parsed ${records.length} raw record(s)`);
 
        console.log('\nProcessing records...');
        const { validRecords, stats } = processInventory(records);
 
        // Write cleaned data
        console.log('\nWriting cleaned data...');
        fs.writeFileSync(
            OUTPUT_FILE,
            JSON.stringify(validRecords, null, 2),
            'utf8'
        );
 
        // Print statistics
        console.log('\n=== ETL Statistics ===');
        console.log(`Total records processed:       ${stats.total}`);
        console.log(`Invalid records skipped:       ${stats.invalid}`);
        console.log(`Duplicate SKUs found/handled:  ${stats.duplicates}`);
        console.log(`Records in final clean output: ${stats.clean}`);
        console.log(`\nOutput written to: ${OUTPUT_FILE}`);
 
    } catch (error) {
        console.error('ETL process failed:', error.message);
        process.exit(1);
    }
}
 
await runETL();
