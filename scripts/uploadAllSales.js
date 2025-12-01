// uploadAllSales.js - FIXED VERSION
const { createClient } = require('@supabase/supabase-js');
const csv = require('csv-parser');
const fs = require('fs');
require('dotenv').config();

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

class SalesUploader {
  static standardizePassengerKey(inputKey) {
    if (!inputKey || typeof inputKey !== 'string') return null;
    if (!inputKey.includes('P')) return null;
    const numbersOnly = inputKey.replace(/\D/g, '');
    if (numbersOnly.length < 3) return null;
    return `P${numbersOnly.slice(-3).padStart(3, '0')}`;
  }

  static standardizeAmount(amount) {
    if (!amount) return 0.00;
    const clean = String(amount).replace(/[^\d.]/g, '');
    const parsed = parseFloat(clean);
    return isNaN(parsed) ? 0.00 : parseFloat(parsed.toFixed(2));
  }

  static standardizeDate(dateStr) {
    try {
      const date = new Date(dateStr);
      if (isNaN(date.getTime())) return null;
      return date.toISOString().split('T')[0];
    } catch {
      return null;
    }
  }

  static async processSalesFile(filePath, sourceType) {
    const sales = [];
    const dirtyRows = [];
    const seenTransactionIds = new Set(); // Track duplicates within file

    await new Promise((resolve, reject) => {
      fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
          try {
            // Get original transaction ID for duplicate checking
            const originalTransactionId = row.TransactionID?.toString().trim();

            // Create standardized transaction ID with prefix
            let transactionId;
            if (sourceType === 'travel_agency') {
              const numbers = originalTransactionId?.replace(/\D/g, '') || '0';
              transactionId = 'TA' + numbers.padStart(6, '0');
            } else {
              const numbers = originalTransactionId?.replace(/\D/g, '') || '0';
              transactionId = 'CO' + numbers.padStart(6, '0');
            }

            // Check for duplicate transaction ID within this file
            if (seenTransactionIds.has(transactionId)) {
              dirtyRows.push({
                source_table: sourceType,
                original_data: row,
                error_reason: `Duplicate transaction ID within file: ${transactionId}`
              });
              return;
            }
            seenTransactionIds.add(transactionId);

            // Standardize passenger key
            const passengerKey = this.standardizePassengerKey(
              row.PassengerID || row.PassengerKey
            );

            if (!passengerKey) {
              dirtyRows.push({
                source_table: sourceType,
                original_data: row,
                error_reason: 'Invalid passenger key'
              });
              return;
            }

            // Validate flight exists
            const flightKey = row.FlightID || row.FlightKey;
            if (!flightKey) {
              dirtyRows.push({
                source_table: sourceType,
                original_data: row,
                error_reason: 'Missing flight key'
              });
              return;
            }

            // Standardize date
            const standardizedDate = this.standardizeDate(
              row.TransactionDate || row.DateKey
            );

            if (!standardizedDate) {
              dirtyRows.push({
                source_table: sourceType,
                original_data: row,
                error_reason: 'Invalid date'
              });
              return;
            }

            const dateKey = parseInt(standardizedDate.replace(/-/g, ''));

            // Validate required numeric fields
            if (!row.TicketPrice && row.TicketPrice !== '0') {
              dirtyRows.push({
                source_table: sourceType,
                original_data: row,
                error_reason: 'Missing ticket price'
              });
              return;
            }

            sales.push({
              transaction_id: transactionId,
              date_key: dateKey,
              passenger_key: passengerKey,
              flight_key: flightKey,
              ticket_price: this.standardizeAmount(row.TicketPrice),
              taxes: this.standardizeAmount(row.Taxes || 0),
              baggage_fees: this.standardizeAmount(row.BaggageFees || 0),
              total_amount: this.standardizeAmount(row.TotalAmount),
              sales_source: sourceType
            });

          } catch (error) {
            dirtyRows.push({
              source_table: sourceType,
              original_data: row,
              error_reason: `Processing error: ${error.message}`
            });
          }
        })
        .on('end', resolve)
        .on('error', reject);
    });

    return { sales, dirtyRows };
  }

  static async uploadAllSales() {
    console.log('Starting sales data upload...');

    // Process travel agency sales
    console.log('Processing travel agency sales...');
    const { sales: travelSales, dirtyRows: travelDirty } = 
      await this.processSalesFile('datasets/travel_agency_sales_001.csv', 'travel_agency');

    // Process corporate sales  
    console.log('Processing corporate sales...');
    const { sales: corporateSales, dirtyRows: corporateDirty } = 
      await this.processSalesFile('datasets/corporate_sales.csv', 'corporate');

    // Combine all sales
    let allSales = [...travelSales, ...corporateSales];
    const allDirty = [...travelDirty, ...corporateDirty];

    // Check for duplicates ACROSS files and remove them
    const seenIds = new Set();
    const finalSales = [];
    const crossFileDuplicates = [];

    for (const sale of allSales) {
      if (seenIds.has(sale.transaction_id)) {
        crossFileDuplicates.push({
          source_table: sale.sales_source,
          original_data: sale,
          error_reason: `Duplicate transaction ID across files: ${sale.transaction_id}`
        });
      } else {
        seenIds.add(sale.transaction_id);
        finalSales.push(sale);
      }
    }

    // Add cross-file duplicates to dirty data
    if (crossFileDuplicates.length > 0) {
      allDirty.push(...crossFileDuplicates);
    }

    console.log(`Total clean records: ${finalSales.length}`);
    console.log(`Total dirty records: ${allDirty.length}`);
    if (crossFileDuplicates.length > 0) {
      console.log(`Cross-file duplicates found: ${crossFileDuplicates.length}`);
    }

    // Upload clean sales in smaller batches with individual conflict handling
    if (finalSales.length > 0) {
      console.log('\nUploading clean sales data...');

      const batchSize = 100; // Smaller batch size
      let uploaded = 0;
      let errors = 0;
      let skippedDuplicates = 0;

      for (let i = 0; i < finalSales.length; i += batchSize) {
        const batch = finalSales.slice(i, i + batchSize);
        const batchNum = Math.floor(i / batchSize) + 1;

        console.log(`Processing batch ${batchNum} (${batch.length} records)...`);

        try {
          // Try batch upsert first
          const { error } = await supabase
            .from('fact_sales')
            .upsert(batch, { onConflict: 'transaction_id' });

          if (error) {
            console.log(`  Batch ${batchNum} failed, trying individual inserts...`);

            // Insert one by one
            for (const sale of batch) {
              try {
                const { error: singleError } = await supabase
                  .from('fact_sales')
                  .upsert(sale, { onConflict: 'transaction_id' });

                if (singleError) {
                  if (singleError.code === '23505' || singleError.message.includes('duplicate')) {
                    // Already exists in database
                    skippedDuplicates++;
                    console.log(`    ⚠️  ${sale.transaction_id}: Already exists in database`);

                    // Move to dirty table
                    allDirty.push({
                      source_table: sale.sales_source,
                      original_data: sale,
                      error_reason: `Transaction ID already exists in database: ${sale.transaction_id}`
                    });
                  } else {
                    errors++;
                    console.error(`    ✗ ${sale.transaction_id}:`, singleError.message);

                    // Move to dirty table
                    allDirty.push({
                      source_table: sale.sales_source,
                      original_data: sale,
                      error_reason: `Upload failed: ${singleError.message}`
                    });
                  }
                } else {
                  uploaded++;
                  console.log(`    ✓ ${sale.transaction_id}: Added`);
                }
              } catch (singleErr) {
                errors++;
                console.error(`    ✗ ${sale.transaction_id}:`, singleErr.message);
              }

              // Small delay
              await new Promise(resolve => setTimeout(resolve, 50));
            }
          } else {
            uploaded += batch.length;
            console.log(`  ✓ Batch ${batchNum}: ${batch.length} records uploaded`);
          }

          // Small delay between batches
          await new Promise(resolve => setTimeout(resolve, 200));

        } catch (batchError) {
          console.error(`  ✗ Batch ${batchNum} error:`, batchError.message);
          errors += batch.length;
        }
      }

      console.log(`\n✅ UPLOAD SUMMARY`);
      console.log(`Uploaded: ${uploaded} records`);
      console.log(`Skipped (duplicates in DB): ${skippedDuplicates}`);
      console.log(`Errors: ${errors}`);
    }

    // Save dirty data
    if (allDirty.length > 0) {
      console.log(`\nSaving ${allDirty.length} dirty records...`);

      // Save in batches to avoid large inserts
      const dirtyBatchSize = 100;
      let dirtySaved = 0;

      for (let i = 0; i < allDirty.length; i += dirtyBatchSize) {
        const batch = allDirty.slice(i, i + dirtyBatchSize);
        const { error } = await supabase
          .from('dirty_data')
          .insert(batch);

        if (error) {
          console.error(`Error saving dirty batch:`, error.message);
          // Save to local file as backup
          fs.appendFileSync('dirty_sales_backup.json', 
            JSON.stringify({ timestamp: new Date().toISOString(), batch: batch }) + '\n'
          );
        } else {
          dirtySaved += batch.length;
        }

        await new Promise(resolve => setTimeout(resolve, 100));
      }

      console.log(`Saved ${dirtySaved} dirty records to database`);
    }

    console.log('\nSales upload complete!');
  }
}

// Run with error handling
(async () => {
  try {
    await SalesUploader.uploadAllSales();
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
})();