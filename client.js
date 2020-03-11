const { execSync } = require("child_process");
const fs = require("fs");
const rimraf = require('rimraf');
const AWS = require("aws-sdk");
const axios = require("axios");
const FormData = require("form-data");
const readlineSync = require("readline-sync");


// ~~~~~~~~~~~~~~~~~~~ ADJUST THIS TO PULL FROM DIFFERENT BUCKETS ~~~~~~~~~~~~~~~~~~~
// We currently require the client application to configure their AWS access on the 
// console manually before they can use our client.
const s3 = new AWS.S3({
  accessKeyId: AWS.config.accessKeyId,
  secretAccessKey: AWS.config.secretAccessKey,
  region: "us-west-2"
});
const s3Bucket = 'markyu2.0';
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


// Command line arguments
const args = process.argv.slice(2);

const blockSize = 128; // In megabytes
const fileSplitPrefix = "-sf-part-";
const nameNodeURL = args[0];
const localFilesPath = __dirname + "/localFiles/";
const localS3FilesPath = __dirname + "/s3Files/";


console.log("====================Welcome to SUFS====================");

async function processUser() {

  let active = true;
  while (active) {
    console.log("================List of available options==============");
    console.log("1. List all the file in your S3 Bucket");
    console.log("2. Create and write a file to SUFS");
    console.log("3. List all files in SUFS");
    console.log("4. List information of a file in SUFS");
    console.log("5. Download a file from SUFS");
    console.log("Type q to Quit the program");
    let operation = readlineSync.question("Select an operation: ");

    switch (operation) {
      case "1":
        await listFilesInS3();
        break;
      case "2":

        // List files in S3 bucket
        await listFilesInS3();

        resetLocalS3Directory();

        try {

          // Download file from bucket and write to disk
          let filename = await downloadFromBucket();
          console.log("Successfully downloaded file " + filename + " from S3.");

          // Get file size (in megabytes) and determine block size
          let size = getFileSize(localS3FilesPath + filename);
          let numBlocks = Math.ceil(size / (blockSize));

          // Split file in local directory
          let splitFilePartPaths = splitFile(filename);

          // // Write file to namenode and recieve file + block metadata
          let fileMetadata = await  sendRequestToNameNode (filename, size, numBlocks);
          let { BlockIDs, NodeList } = fileMetadata;
          await writeBlocksToDataNode(splitFilePartPaths, BlockIDs, NodeList);
        }
        catch (exception) {
          console.error("Error occured in file creation/writing! STOPPING PROCESS");
          console.log("Please try again in a few moments. ");
        }

        // resetLocalS3Directory();

        break;
      case "3":
        //List files in SUFS, and the data node associated with the filename
        await listFilesInSUFS();
        break;
      case "4":
        await listFilesInSUFS();
        let filename = null;
        try {
          filename = readlineSync.question("Select the file you'd like to list: ");
          let info = await getBlockLocations(filename);
          let blockIDs = info["BlockIDs"];
          let NodeList = info['NodeList'];
          console.log("=======================================================");
          console.log("NodeID","\t\t\t","DataNode");
          for (let i = 0;i < blockIDs.length;i++){
            console.log(blockIDs[i],'\t\t\t',NodeList[i]);
          }
          console.log("=======================================================");

        } catch (error) {
          console.log("Fail to retreive information for file:", filename)
        }

        break;
      case "5":

        // List files in SUFS
        await listFilesInSUFS();

        try {

          // Get filename of file to download
          let filename = readlineSync.question("Enter the file name you'd like to write:");

          // Get blocks IDs and datanode urls from file in SUFS
          let blockLocations = await getBlockLocations(filename);
          let { BlockIDs, NodeList } = blockLocations;
          // Get blocks from SUFS
          let blocks = await getBlocks(BlockIDs, NodeList);

          // Concatenate blocks stored locally
          let filepath = await mergeBlocks(blocks, filename);

          console.log("Successfully wrote file to: " + filepath);
        }
        catch(exception) {
          console.error("Error occured in file downloading/merging! STOPPING PROCESS");
          console.error("Please try again in a few moments.");
        }

        break;

      case "q":
        console.log("Thank you for using SUFS!");
        console.log("Hope you have an excellent day!");

        active = false;
        break;

      default:
        console.log("Invalid input, please try again!");
        break;
    }
  }
}
processUser();


/**
 * Lists all files in S3 bucket in the console.
 */
async function listFilesInS3() {
  let params = {
    Bucket: s3Bucket
  };
  try {
    let files = await s3.listObjects(params).promise();
    console.log("");
    console.log("\tHere is a list of files in the S3 Bucket");
    console.log("=======================================================");
    files.Contents.forEach(key => console.log("\t\t", key.Key));
    console.log("=======================================================");

    return true;
  } catch (error) {
    console.error("Something went wrong when listing the files from the S3");
    console.error("S3 Error: " + error);

    return false;
  }
}


/**
 * Lists all files in SUFS bucket in the console.
 */
async function listFilesInSUFS() {

  return await axios({
    method: "get",
    url: nameNodeURL + "/Files/"
  })
  .then(response => {
    let files =  response.data;
    console.log("");
    console.log("\tHere is a list of files in SUFS");
    console.log("=======================================================");
    files.forEach(file => console.log("\t\t", file));
    console.log("=======================================================");
    return true;
  })
  .catch(error => {
    console.error("Error occured while retrieving filenames from the NameNode!");
    console.error(`Status: ${error.response.status}. Message: ${error.response.data}`);
    return false;
  });

}


/**
 * Download from s3 bucket to local machine
 */
async function downloadFromBucket() {
  let filename = null;

  let done = false;
  while (!done) {
    filename = readlineSync.question(
      "Enter the file name you'd like to write to SUFS:"
    );
    done = await checkExistInBucket(filename);
  }

  params = {
    Bucket: s3Bucket,
    Key: filename
  };

  console.log("Starting file download..........");
  const s3WriteResult = new Promise((resolve, reject) => {
    s3.getObject(params)
      .createReadStream()
      .on("end", () => {
        console.log("File downloaded successfully!");
        resolve(filename);
      })
      .on("error", error => {
        console.error("Failed to download file from S3:");
        throw "S3 Error: " + error;
      })
      .pipe(fs.createWriteStream(localS3FilesPath + filename));
  });

  return await s3WriteResult;
}


/**
 * Send file write request to NameNode. Get back blockIDs and block DataNode locations.  
 */
async function sendRequestToNameNode (filename, size, numBlocks) {

  let fileInfo = {
    FileName: filename,
    FileSize: size,
    BlockCount: numBlocks
  };

  return await axios({
    method: "post",
    url: nameNodeURL + "/Files/",
    data: fileInfo
  })
    .then(response => {
      return response.data;
    })
    .catch(error => {
      console.error("Error occured when communicating with the NameNode!");
      console.error(`Status: ${error.response.status}. Message: ${error.response.data}`);
      throw `Status: ${error.response.status}. Message: ${error.response.data}`;
    });
}


/**
 * Write blocks to declared DataNodes.
 */
async function writeBlocksToDataNode(localBlockPaths, blockIDs, dataNodeLists) {

  console.log("Now writing " + localBlockPaths.length + " blocks to SUFS...")

  let uploadRequestList = [];
  for (let i = 0; i < blockIDs.length; i++) {
    uploadRequestList.push(writeBlockToDataNode(
      localBlockPaths[i],
      blockIDs[i],
      dataNodeLists[i][0],
      dataNodeLists[i].splice(1)
    ));
  }

  await Promise.all(uploadRequestList);

  console.log("All blocks written to SUFS!");
}


/**
 * Writes individual block to DataNode, returns true if sucessful. Returns axios promise that must be awaited.
 */
function writeBlockToDataNode(localBlockPath, blockID, dataNodeURL, nextDataNodeURLs) {
  let uploadURL = dataNodeURL + "/Blocks/" + blockID + "/";

  let form = new FormData();
  form.append("block-" + blockID, fs.createReadStream(localS3FilesPath + localBlockPath));
  form.append("next", nextDataNodeURLs.join());

  return axios({
    method: "put",
    url: uploadURL,
    data: form,
    maxContentLength: 1000000000,
    maxBodyLength: 1000000000,
    headers: { ...form.getHeaders() }
  })
    .then(function (response) {
      if (response.status == 200) {
        console.log("Block " + blockID + " sent successfully!");
        return true;
      }
    })
    .catch(function(error) {
      throw `Error occured uploading block ${blockID}. Status : ${error.response.status}. Message : ${error.response.data}`;
    });
}


/**
 * Confirms local S3 files directory exists and clears its contents.
 */
function resetLocalS3Directory() {
  if (!fs.existsSync(localS3FilesPath)) {
    fs.mkdirSync(localS3FilesPath);
  }
  rimraf(localS3FilesPath + "*", () => {
    // Reset local S3 Directory
  });
}


/**
 * Checks if the declared file is in the S3 bucket.
 * @param {string} filename name of the file we want to download.
 */
async function checkExistInBucket(filename) {
  let params = {
    Bucket: s3Bucket,
    Key: filename
  };

  try {
    await s3.headObject(params).promise();
    return true;
  } catch (error) {
    console.error(`Error occured in retrieving file. File ${filename} not found in the cloud.`);
    throw `Error occured in retrieving file. File ${filename} not found in the cloud.`;
  }
}


/**
 * Get the size of the file in megabytes only if it exists.
 * @param {string} filename
 */
function getFileSize(filename) {

  if (!fs.existsSync(filename)) {
    throw "Cannot determine size of file. File does not exists!";
  }

  let stats = fs.statSync(filename);
  let fileSize = stats["size"] / (1024.0 * 1024.0);
  return fileSize;
}


/**
 * Splits the file and returns an array of the filenames of each of the file parts
 */
function splitFile(filename) {
  let filepath = localS3FilesPath + filename;
  if (!fs.existsSync(filepath)) {
    throw new Error("Error occured in file split. The file does not exist!");
  }

  let prefix = localS3FilesPath + filename + fileSplitPrefix;
  let command = `split -b ${blockSize}m  ${filepath} ${prefix}`;

  execSync(command);

  return fs.readdirSync(localS3FilesPath).filter(file => file.includes(fileSplitPrefix));
}


/**
 * Returns the locations (ID and dataNode URL) of blocks associated to a file.
 */
async function getBlockLocations(filename) {

  return await axios({
    method: "get",
    url: nameNodeURL + "/Nodes/" + filename,
  })
  .then(function (response) {
    return response.data;
  })
  .catch(function (error) {
    console.error("An error occured getting the block locations of file: " + filename);
    console.error(`Status: ${error.response.status}. Message: ${error.response.data}`);

    throw `Status: ${error.response.status}. Message: ${error.response.data}`;
   });
}


/**
 * Downloads blocks to disk and returns their filepaths. 
 */
async function getBlocks(blockIDs, dataNodeURLs) {

  if (!fs.existsSync(localFilesPath)) {
    fs.mkdirSync(localFilesPath);
  }

  let blockRequestList = [];
  for (let i = 0; i < blockIDs.length; i++) {
    blockRequestList.push(
      getBlockFromDataNode(blockIDs[i], dataNodeURLs[i])
    );
  }

  return await Promise.all(blockRequestList);

}


/**
 * Downloads block from datanode and saves it locally on the disk. Returns path of block.
 */
async function getBlockFromDataNode(blockID, dataNodeURL) {
  return await axios({
    method: "get",
    url: dataNodeURL + "/Blocks/" + blockID,
    responseType: 'arraybuffer',
    responseEncoding: 'binary',
  })
    .then(function (response) {
      let localBlockPath = localFilesPath + blockID + ".block";

    fs.writeFileSync(localBlockPath, response.data);

    return localBlockPath;
  })
  .catch(function (error) {
    console.error(`An error occured writing the block ${blockID} to disk:`);
    console.error(`Status: ${error.response.status}. Message: ${error.response.data}`);
    throw `Status: ${error.response.status}. Message: ${error.response.data}`;
  });
}


/**
 * Mergers blocks using system `cat` call and returns file path. 
 */
async function mergeBlocks(blockPaths, fileName) {
  let finalFilePath = localFilesPath + fileName;
  let command = "cat " + blockPaths.join(' ') + " >> " + finalFilePath;
  execSync(command);

  return finalFilePath;
}