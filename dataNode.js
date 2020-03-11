const fs = require("fs");
const FormData = require("form-data");
const axios = require("axios");
const express = require("express");
const bodyParser = require("body-parser");
const multer = require("multer");

// Command line arguments

const args = process.argv.slice(2);

// DataNode Properties
const nodeDNS = args[1];
const NodeID = parseInt(args[0]);
const PORT = 3000 + NodeID;
const nameNodeURL = args[2];

// Array of blockIDs refrerring to blocks that are stored locally on disk.
const blocks = [];

// Storage engine for block uploads
var storage = multer.diskStorage({
  destination: function(req, file, cb) {
    if (!fs.existsSync("./DataNode-" + NodeID + "/")) {
      fs.mkdirSync("./DataNode-" + NodeID + "/");
      fs.mkdirSync("./DataNode-" + NodeID + "/blocks/");
    }
    cb(null, "./DataNode-" + NodeID + "/blocks/");
  },
  filename: function(req, file, cb) {
    cb(null, file.fieldname + ".block");
  }
});

// Express plugin to recieve files in formdata
let upload = multer({ storage: storage });

// Initialize express app
const app = express();
app.use(bodyParser.json());

// Tells datanode to replicate a specicific block to a target datanode.
// Expects the target datanode URL to be in the body.
app.post("/Replicate/:BlockID/", function(req, res) {
  let blockID = req.params.BlockID;
  let nodeURL = req.body.NodeURL;

  if (!nodeURL || !blockID) {
    res.status(400).send("Replication request malformed.");
  }

  sendDataBlock(nodeURL, blockID);

  console.log("Replication operation recieved.");
  res.send("Replication Operation Recieved.");
});

// Saves block to disk and remembers that the blockID is stored at this node.
// Expects block file to be in the body.
app.put("/Blocks/:BlockID", upload.any(), function(req, res) {
  let blockID = req.params.BlockID;
  let file = req.files[0];

  let nextDataNodes = null;
  if (req.body.next != null) {
    nextDataNodes = req.body.next.split(",");
  }

  if (file == null) {
    res.status(400).send("No block file uploaded.");
  } else {
    console.log("Trying to write: " + file.fieldname);
    // Wait for block to finish writing
    while (
      !fs.existsSync(
        "./DataNode-" + NodeID + "/blocks/" + file.fieldname + ".block"
      )
    ) {}

    fs.renameSync(
      "./DataNode-" + NodeID + "/blocks/" + file.fieldname + ".block",
      "./DataNode-" + NodeID + "/blocks/" + blockID + ".block"
    );
    blocks.push(blockID);

    // If relpications need to be made
    if (nextDataNodes != null) {
      let nextDataNode = nextDataNodes[0];

      sendDataBlock(nextDataNode, blockID, nextDataNodes.slice(1));
    }

    res.send("Block upload succeeded!");
  }
});

// Returns a specific block declared in the request.
app.get("/Blocks/:BlockID", function(req, res) {
  let blockID = req.params.BlockID;
  let blockPath = __dirname + "/DataNode-" + NodeID + "/blocks/" + blockID + ".block";

  if (!fs.existsSync(blockPath)) {
    res.status(404).send("Block not found.");
  } else {
    res.download(blockPath, blockID + ".block");
  }
});

// Listen for requests
app.listen(PORT, () => console.log(`DataNode ${NodeID} listening on port ${PORT}!`));

// Time between block reports in seconds
const blockReportInterval = 30;

// Send block report to nameNode on interval of [blockReportInterval] seconds.
setInterval(() => {
  const blockReportURL = nameNodeURL + "/BlockReport/" + NodeID;

  axios({
    method: "put",
    url: blockReportURL,
    data: {
      blocks
    },
    headers: {
      referer: nodeDNS
    }
  })
    .then(function(response) {
      if (response.status == 200) {
        console.log("Block report sent successfully!");
      }
    })
    .catch(function(error) {
      console.log(error);
      console.error("Block report sending failed!");
    });
}, blockReportInterval * 1000);

function sendDataBlock(nodeURL, blockID, nextDataNodes = []) {
  let uploadURL = nodeURL + "/Blocks/" + blockID + "/";

  let form = new FormData();
  form.append(
    "block-" + blockID,
    fs.createReadStream(
      "./DataNode-" + NodeID + "/blocks/" + blockID + ".block"
    )
  );

  if (nextDataNodes.length > 0) {
    form.append("next", nextDataNodes.join());
  }

  axios({
    method: "put",
    url: uploadURL,
    data: form,
    maxContentLength: 128000000000,
    maxBodyLength: 128000000000,
    headers: { ...form.getHeaders() }
  })
    .then(function(response) {
      if (response.status == 200) {
        console.log("Replication occured successfully!");
      }
    })
    .catch(function(error) {
      console.error("Replication Error Occurred: " + error);
    });
}
