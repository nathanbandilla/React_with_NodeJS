import https from 'https';
import fs from 'fs';


// Configuration
const OUTPUT_FILE = 'cleaned_inventory.json';

// Validation helpers
const isValidId = (id) => {
    return id && typeof id === 'string' && id.length > 0;
};

const isValidSku = (sku) => {
    return sku && typeof sku === 'string' && sku.startsWith('SKU-');
};

const isValidPrice = (price) => {
    if (typeof price === 'string' && price === 'N/A') return false;
    const num = parseFloat(price);
    return !isNaN(num) && num >= 0;
};

const isValidQuantity = (qty) => {
    return typeof qty === 'number' && qty >= 0 && Number.isInteger(qty);
};

const cleanName = (name) => {
    if (!name || typeof name !== 'string') return null;
    let cleaned = name.trim();
    cleaned = cleaned.replace(/^\?+/, '');
    return cleaned.length > 0 ? cleaned : null;
};

const normalizeCategory = (cat) => {
    const validCategories = ['tools', 'apparel', 'electronics', 'kitchen', 'outdoor', 'toys', 'books'];
    return validCategories.includes(cat) ? cat : 'uncategorized';
};

// Fetch data from remote server
const fetchInventory = (url) => {
    return new Promise((resolve, reject) => {
        https.get(url, (res) => {
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
    const seenSkus = new Map();
    const validRecords = [];
    const stats = {
        total: 0,
        invalid: 0,
        duplicates: 0,
        cleaned: 0
    };

    for (const record of records) {
        stats.total++;

        // Validate required fields
        if (!isValidId(record.id)) {
            stats.invalid++;
        }

        if (!isValidSku(record.sku)) {
            stats.invalid++;
        }

        if (!isValidPrice(record.price)) {
            stats.invalid++;
        }

        if (!isValidQuantity(record.quantity)) {
            stats.invalid++;
        }

        const cleanedName = cleanName(record.name);
        if (!cleanedName) {
            stats.invalid++;
        }


        if (seenSkus.has(record.sku)) {
            stats.duplicates++;
        }

        seenSkus.set(record.sku, true);

        const cleanedRecord = {
            id: record.id,
            sku: record.sku,
            name: cleanedName,
            price: record.price,
            quantity: record.quantity,
            category: normalizeCategory(record.category),
            last_updated: record.last_updated
        };

        stats.cleaned++;
        validRecords.push(cleanedRecord);
    }

    return { validRecords, stats };
}

// Main ETL process
async function runETL() {
    const remoteUrl = process.argv[2];
    if (!remoteUrl) {
        console.error('Usage: node process_json.js <REMOTE_URL>');
        process.exit(1);
    }
    console.log('Starting ETL process...');
    console.log(`Fetching data from: ${remoteUrl}`);

    try {
        const rawData = await fetchInventory(remoteUrl);
        console.log(`Downloaded ${rawData.length} bytes`);

        const records = JSON.parse(rawData);

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
        console.log(`Total records processed: ${stats.total}`);
        console.log(`Invalid records: ${stats.invalid}`);
        console.log(`Duplicate SKUs: ${stats.duplicates}`);
        console.log(`Clean records: ${stats.cleaned}`);
        console.log(`\nOutput written to: ${OUTPUT_FILE}`);

    } catch (error) {
        console.error('ETL process failed:', error.message);
        process.exit(1);
    }
}

await runETL();