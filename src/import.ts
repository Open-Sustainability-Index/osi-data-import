import * as fs from 'fs';
import { Client } from 'pg';
import * as dotenv from 'dotenv';
import { toSnakeCase, checkHeaders } from './utils';
const csv = require('csv-parser'); // Use require instead of import

dotenv.config();

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error('DATABASE_URL is not set in the environment variables.');
  process.exit(1);
}

const client = new Client({
  connectionString: DATABASE_URL
});

client.connect();

const csvFilePath = './data/companies.csv';

const results: any[] = [];

fs.createReadStream(csvFilePath)
  .pipe(csv())
  .on('headers', (headers: string[]) => {
    const snakeCaseHeaders = headers.map(toSnakeCase);
    const missingHeaders = checkHeaders(snakeCaseHeaders);

    if (missingHeaders.length > 0) {
      console.error(`Missing headers: ${missingHeaders.join(', ')}`);
      process.exit(1);
    }

    console.log('Headers:', snakeCaseHeaders);
  })
  .on('data', (data: any) => results.push(data))
  .on('end', async () => {
    try {
      for (const row of results) {
        const snakeCaseRow = Object.keys(row).reduce((acc: Record<string, any>, key) => {
          acc[toSnakeCase(key)] = row[key];
          return acc;
        }, {} as Record<string, any>);

        const columns = Object.keys(snakeCaseRow).join(', ');
        const values = Object.values(snakeCaseRow);
        const valuePlaceholders = values.map((_, i) => `$${i + 1}`).join(', ');

        const query = `INSERT INTO company (${columns}) VALUES (${valuePlaceholders})`;

        await client.query(query, values);
      }

      console.log('Data imported successfully.');
    } catch (error) {
      console.error('Error inserting data:', error);
    } finally {
      client.end();
    }
  });