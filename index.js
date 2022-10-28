import XLSX from 'xlsx';
import fs from 'fs';
import { readdir } from 'fs/promises'

import path, { dirname } from 'path';
import { fileURLToPath } from 'url';
import fsExtra from 'fs-extra'

import minimist from 'minimist';

import Path from "path";
import FS from "fs";

import unzipper from 'unzipper';

const args = minimist(process.argv.slice(2));

const objectArgs = args["_"].reduce((prev, cur) => {
  const [key, value] = cur.split(":");
  return { ...prev, [key]: value }
}, {});

console.log(objectArgs)

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

//step1: empty output folder
const emptyit = () => {
  fsExtra.remove('./extracted')
  fsExtra.remove('./outputs')
  fsExtra.remove('./errors')
  fsExtra.remove('./all.json')
  fsExtra.remove('./finalEport.xlsx')
  fsExtra.remove('./extracted')
  fsExtra.remove('./errors')
  fsExtra.remove('./all.json')
  fsExtra.remove('./relevant.json')
  fsExtra.remove('./keys.txt')
  fsExtra.remove('./refined.json')
}




//step2: read all files in input folder convert to json and save to output folder
const toOutPutFolder = async () => {




  let files = [];




  if (objectArgs.zipped === "true") {
    console.log(4567);
    const dir = __dirname + '/extracted';
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir);
    }


    fs.createReadStream('./inputs/data.zip')
      .pipe(unzipper.Extract({ path: `${__dirname}/extracted` }));

  }

  const throughDirectory = (Directory) => {
    FS.readdirSync(Directory).forEach(File => {
      const Absolute = Path.join(Directory, File);
      if (FS.statSync(Absolute).isDirectory()) return throughDirectory(Absolute);
      else return files.push(Absolute);
    });
  }
  const inputDir = objectArgs.zipped ? `${__dirname}/extracted` : `${__dirname}/inputs`;
  throughDirectory(inputDir);
  const xlsFiles = files.filter(file => file.endsWith('.xls') || file.endsWith('.xlsx'));
  // Loop through all the files and convert them to json
  const dir = __dirname + '/outputs';
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir);
  }
  xlsFiles.forEach((file, index) => {
    try {
      const workbook = XLSX.readFile(file);
      const sheet_name_list = workbook.SheetNames;
      const json = XLSX.utils.sheet_to_json(workbook.Sheets[sheet_name_list[0]]);
      const toWrite = json.map((item, index) => ({ ...item, source: file }))
      //create a new json file with the same name as the xls file
      const newDir = file.replace(/\//g, '_');

      fs.writeFileSync(`${__dirname}/outputs/${newDir}.json`, JSON.stringify(toWrite, null, 4), {}, (err) => {
      });


    } catch (error) {

    }

  });


}



//step 3 loop through output folder if file is an empty array move it to error folder
const moveEmptyFiles = async () => {
  const dir = __dirname + '/errors';
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir);
  }
  const files = await readdir('./errors');
  // console.log(files);
  files.forEach(async (file, index) => {
    const json = JSON.parse(fs.readFileSync(`./outputs/${file}`));
    if (json.length === 0) {
      fs.renameSync(`./outputs/${file}`, `./errors/${file}`);
    }
  })

}

//step 4 write in txt file names of files in error folder
const writeErrorFiles = async () => {
  const files = await readdir('./errors');
  const errorFiles = files.forEach(async (file, index) => {
    fs.appendFileSync('./errors/error.txt', `${file} \n`, (err) => {
      if (err) throw err;
    });
  })
}

//step 5 loop through output and write it in on json file
const writeAllFiles = async () => {
  const files = await readdir('./outputs');
  console.log(files.length);
  const bigArray = await Promise.all(files.map(async (file, index) => {
    try {
      const json = JSON.parse(fs.readFileSync(`./outputs/${file}`));
      return json;


    } catch (error) {
      console.log(file);
    }
  }))
  const bigarrayInOne = bigArray.flat();
  console.log(bigarrayInOne.length);
  const chunkLength = 100;
  for (let index = 0; index < bigarrayInOne.length; index += chunkLength) {
    const chunk = bigarrayInOne.slice(index, index + chunkLength);
    if (index === 0) fs.writeFileSync(`${__dirname}/all.json`, `[\n`, (err) => { })
    fs.appendFileSync(`${__dirname}/all.json`, `${index !== 0 ? "," : ""}\n${JSON.stringify(chunk, null, 4)}`, (err) => { });
    if ((bigarrayInOne.length - 1 - index) <= chunkLength) fs.appendFileSync(`${__dirname}/all.json`, `\n]`, (err) => { });

  }

}
//step 6 get all keys from the json array .all.json  and write it in a txt file
const writeAllKeys = async (file) => {
  const json = JSON.parse(fs.readFileSync(file));
  const arr = json.flat();
  const keys = arr.reduce((acc, curr) => {
    // console.log(Object.keys(curr))
    return [
      ...acc,
      ...Object.keys(curr).filter(key => !acc.includes(key))
    ]
  }
    , [])
  keys.forEach((key, index) => {
    fs.appendFileSync('./keys.txt', `${key} \n`, (err) => {
      if (err) throw err;
    });
  })
}

// step 7 remove all elems that have none of the array keys
const removeElems = async (file) => {
  const arrayKeys = [
    "Prénom",
    "Nom",
    "Code Postal",
    "Ville",
    "Tél. mobile",
    "Email",
    "Tél. fixe",
    "Adresse",
    "CP",
    "el",
    "mail",
    "code postal",
    "ville",
    "adresse",
    "nom",
    "prenom",
    "tel1",
    "tel2",
    "tel3",
    "mobile",
    "tel1_prospection",
    "tel2_prospection",
    "tel3_prospection",
    "mobile_prospection",
    "Tél fixe",
    "Tél  mobile",
    "Tél bureau",
    "Tél",
    "GSM",
    "Bureau",
    "Tel",
    "Cp",
    "Mobile",
    "el1",
    "tel",
    "Nom ",
    "Adresse ",
    "Tel portable",
    "PRENOM",
    "NOM",
    "ADRESSE",
    "CODE_POSTAL",
    "TELEPHONE",
    "VILLE",
    "AUTRE FIXE",
    "NUMEROPORTABLE",
    "EMAIL",
    "cp",
    "cp(numerique!!)",
    "Prénom",
    "Nom",
    "Code Postal",
    "Ville",
    "Tél. mobile",
    "Email",
    "Adresse du chantier",
    "Ville chantier",
    "Tél. fixe",
    "Adresse",
    "NPN",
    "CP",
    "el",
    "mail",
    "code postal",
    "ville",
    "adresse",
    "nom",
    "prenom",
    "tel1",
    "tel2",
    "tel3",
    "mobile",
    "tel1_prospection",
    "tel2_prospection",
    "tel3_prospection",
    "mobile_prospection",
    "Tél fixe",
    "Tél  mobile",
    "Tél bureau",
    "Tél",
    "GSM",
    "Bureau",
    "Tel",
    "Cp",
    "Mobile",
    "el1",
    "tel",
    "Adresse ",
    "Tel portable",
    "PRENOM",
    "NOM",
    "ADRESSE",
    "CODE_POSTAL",
    "TELEPHONE",
    "VILLE",
    "AUTRE FIXE",
    "NUMEROPORTABLE",
    "EMAIL",
    "AGE",
    "T",
    "cp",
    "cp(numerique!!"
  ]
  const json = JSON.parse(fs.readFileSync(file));
  const arr = json.flat();
  const relevant = arr.filter((elem) => {
    const keys = Object.keys(elem);
    const hasRelevantKey = arrayKeys.some(key => keys.includes(key));
    return hasRelevantKey;
  })
  console.log(relevant.length);
  fs.writeFileSync(`${__dirname}/relevant.json`, JSON.stringify(relevant, null, 4), (err) => { })
}

// step 8 refactor relevant.json
const refactor = async (file) => {
  const mapper = {
    ADRESSE: [
      "ADRESSE",
      "Adresse",
      "Adresse",
      "Adresse ",
      "Adresse ",
      "Adresse du chantier",
      "adresse",
      "adresse",

    ],
    TEL: [
      "AUTRE FIXE",
      "AUTRE FIXE",
      "T",
      "TELEPHONE",
      "TELEPHONE",
      "Tel",
      "Tel",
      "Tel portable",
      "Tel portable",
      "Tél",
      "Tél",
      "Tél  mobile",
      "Tél  mobile",
      "Tél fixe",
      "Tél fixe",
      "Tél. fixe",
      "Tél. fixe",
      "Tél. mobile",
      "Tél. mobile",
      "Bureau",
      "Bureau",
      "Tél bureau",
      "Tél bureau",
      "GSM",
      "GSM",
      "Mobile",
      "Mobile",
      "NUMEROPORTABLE",
      "NUMEROPORTABLE",
      "el",
      "el",
      "el1",
      "el1",
      "mobile",
      "mobile",
      "mobile_prospection",
      "mobile_prospection",
      "tel",
      "tel",
      "tel1",
      "tel1",
      "tel1_prospection",
      "tel1_prospection",
      "tel2",
      "tel2",
      "tel2_prospection",
      "tel2_prospection",
      "tel3",
      "tel3",
      "tel3_prospection",
      "tel3_prospection",
    ],
    CODE_POSTAL: ["CODE_POSTAL",
      "CODE_POSTAL",
      "CP",
      "CP",
      "Code Postal",
      "Code Postal",
      "Cp",
      "Cp",
      "code postal",
      "code postal",
      "cp",
      "cp",
      "cp(numerique!!",
      "cp(numerique!!)",
    ],
    EMAIL: [
      "EMAIL",
      "EMAIL",
      "Email",
      "Email",
      "mail",
      "mail",
    ],
    NOM: [
      "NOM",
      "NOM",
      "NPN",
      "Nom",
      "Nom",
      "Nom ",
      "nom",
      "nom",
      "date(seulement format 01/01/2009)"
    ],
    PRENOM: [
      "PRENOM",
      "PRENOM",
      "Prénom",
      "Prénom",
      "prenom",
      "prenom",
    ],
    VILLE: [
      "VILLE",
      "VILLE",
      "Ville",
      "Ville",
      "Ville chantier",
      "ville",
      "ville"
    ],
    SOURCE: ["source"]
  }
  const json = JSON.parse(fs.readFileSync(file));
  const arr = json.flat();
  const mapperAssrray = Object.entries(mapper);
  const notRelevantValues = ["non", "oui", ""]

  const reWritedKeys = arr.map((elem) => {
    const keys = Object.keys(elem);
    const newElem = {};
    keys.forEach((key) => {
      const value = elem[key];
      const newKey = mapperAssrray.find((elem) => elem[1].includes(key))?.[0];
      if (newKey !== undefined && !notRelevantValues.some((elem) => elem === value)) {
        const sameKeyz = Object.keys(newElem).filter((key) => key.includes(newKey) && key != "PRENOM" && newKey != "NOM").length
        sameKeyz === 0
          ? newElem[newKey] = value
          : newElem[`${newKey}_${sameKeyz}`] = value
      }
    })
    return newElem;

  })

  const orderbyCp = reWritedKeys.sort((a, b) => a.CODE_POSTAL - b.CODE_POSTAL)

  fs.writeFileSync(`${__dirname}/refactored.json`, JSON.stringify(orderbyCp, null, 4), (err) => { })

}


// step 9 retun refactored.json as xlsx
const jsonToXlsx = async (file) => {
  const json = JSON.parse(fs.readFileSync(file));
  const arr = json.flat();
  const workbook = XLSX.utils.book_new();


  const worksheet = XLSX.utils.json_to_sheet(arr);
  XLSX.utils.book_append_sheet(workbook, worksheet, "data");
  XLSX.writeFile(workbook, `${__dirname}/finalEport.xlsx`, { compression: true });


}

console.log("step 1");
emptyit()
console.log("step 2");
await toOutPutFolder()
console.log("step 3");
await moveEmptyFiles()
console.log("step 4");
await writeErrorFiles()
console.log("step 5");
await writeAllFiles()
console.log("step 6");
await writeAllKeys(`./all.json`)
console.log("step 7");
await removeElems(`./all.json`)
console.log("step 8");
await refactor(`./relevant.json`)
console.log("step 9");
await jsonToXlsx(`./refactored.json`)