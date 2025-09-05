import fs from "fs";
import path from "path";
import * as XLSX from "xlsx";
import mongoose from "mongoose";

async function importExcelFiles(folderPath) {
  try {
    // Connect to MongoDB
    await mongoose.connect("mongodb://localhost:27017/clearvue");

    // Read all files in the folder
    const files = fs.readdirSync(folderPath);

    for (const file of files) {
      if (file.endsWith(".xlsx") || file.endsWith(".xls")) {
        const filePath = path.join(folderPath, file);

        // Read workbook
        const workbook = XLSX.readFile(filePath);
        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];

        // Convert sheet to JSON
        const data = XLSX.utils.sheet_to_json(sheet);

        if (data.length > 0) {
          // Use filename (without extension) as collection name
          const collectionName = path
            .basename(file, path.extname(file))
            .toLowerCase();

          // Create a dynamic schema (flexible, since different entities have diff. fields)
          const DynamicModel = mongoose.model(
            collectionName,
            new mongoose.Schema({}, { strict: false }),
            collectionName // use exact collection name
          );

          // Insert data
          await DynamicModel.insertMany(data);
          console.log(
            `Imported ${data.length} records into '${collectionName}' collection`
          );
        }
      }
    }

    console.log("✅ All Excel files imported successfully!");
    mongoose.connection.close();
  } catch (error) {
    console.error("❌ Error importing files:", error);
  }
}

// Change this path to your folder
//make sure the excel files are in your project root directory
const folderPath =
  "C:Users\\Givenchie\\Desktop\\NWU 2025\\SEMESTER 2\\ADV DATABASES-CMPG321\\ProjectClearVueBIProj\\exceldata";
importExcelFiles(folderPath);
