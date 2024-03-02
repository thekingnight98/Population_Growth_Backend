require('dotenv').config();
const express = require('express');
const fs = require('fs');
const csv = require('csv-parser');
const { MongoClient } = require('mongodb');
const app = express();
const port = 9000;
const cors = require('cors');

const JSONStream = require('JSONStream');


const uri = process.env.MONGODB_URI;
let client;
let db;

app.use(cors());

async function connectDB() {
  client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });
  await client.connect();
  db = client.db('CountryDB');
  console.log("Connected to MongoDB Atlas");
}

app.get('/country', async (req, res) => {
  try {
    const country = db.collection('SGNCollection');
    const cursor = country.find({}, { projection: { _id: 0, "Country name": 1, "Year": 1, "Population": 1 } });

    res.status(200);
    // สตรีมข้อมูลจาก Cursor ไปยัง Response
    cursor.stream().pipe(JSONStream.stringify()).pipe(res);
  } catch (e) {
    res.status(500).json({ message: e.message });
  }
});

app.get('/import', async (req, res) => {
  const results = [];
  fs.createReadStream('./population-and-demography.csv')
    .pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', async () => {
      try {
        const collection = db.collection('SGNCollection');
        await collection.insertMany(results);
        console.log("Data has been successfully imported to MongoDB");
        res.status(200).json({ message: "Data has been successfully imported to MongoDB" });
      } catch (e) {
        res.status(500).json({ message: e.message });
      }
    });
});

// ใช้ connectDB() เพื่อเชื่อมต่อกับ MongoDB ก่อนที่ application จะเริ่มรับ requests
connectDB().then(() => {
  app.listen(port, () => {
    console.log(`App listening at http://localhost:${port}`);
  });
}).catch(console.error);
