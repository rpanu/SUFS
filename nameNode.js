const axios = require("axios");
const express = require("express");
const app = express();
const bodyParser = require("body-parser");


const PORT = 8000;


let block_residency = {};
let block_counter = {
  "0": 0,
  "1": 0,
  "2": 0,
  "3": 0,
  "4": 0
};
let report = {};
let files = {};


const START_NODE_COUNT = 5;
const CHECK_INTERVAL = 20;
const REPLICATION_FACTOR = 3;


var current_blockID = 0;


app.use(bodyParser.json());


// takes the filename and its size and stores it on a file list
app.post("/Files/", function(req, res) {
  let fileName = req.body.FileName;
  let fileSize = req.body.FileSize;
  let blockCount = req.body.BlockCount;

  // Validate the request
  if (!fileName || !blockCount) {
    res.status(500).send("Request Failed. Missing Data parameters");
    return null;
  }
  if (Object.keys(report).length == 0) {
    res.status(500).send("DATA NODES ARE DEAD OMG!");
    return null;
  }

  let blockIDs = createBlockIDs(blockCount);
  let nodeList = getNodeList(blockIDs, blockCount);


  files[fileName] = {
    FileSize: fileSize,
    BlockIDs: blockIDs,
    BlockCount: blockCount,
    NodeList: nodeList
  };

  let file = {
    BlockIDs: blockIDs,
    NodeList: nodeList
  };

  console.log("Allocated blocks for the file");

  res.send(file);
});

// Returns all files currently saved in SUFS.
app.get("/Files/", function(req, res) 
{
  res.send(Object.keys(files));
});


// returns all the block IDs associated with the file and which dataNode they are stored at
app.get("/Nodes/:FileName", function(req, res) {
  let fileName = req.params.FileName;

  console.log(files);
  if (!(fileName in files)) {
    res.status(500).send("File Doesn't exist");
    return null;
  }

  let nodes = [];
  for (var i = 0; i < files[fileName].BlockIDs.length; i++) {
    let blockID = files[fileName].BlockIDs[i];
    let nodeID = getNodeIDWithBlockID(blockID);
    nodes.push(report[nodeID].publicDNS);
  }

  let file = {
    BlockIDs: files[fileName].BlockIDs,
    NodeList: nodes
  };

  res.send(file);
});


// acts as heartbeat, updates a node with list of blocks and stores public dns and current date
app.put("/BlockReport/:NodeID", function(req, res) {
  let nodeID = req.params.NodeID;

  report[nodeID] = {
    BlockList: req.body.blocks,
    timestamp: Date.now(),
    publicDNS: req.headers.referer
  };

  for (var i = 0; i < report[nodeID].BlockList.length; i++) {
    var block = report[nodeID].BlockList[i];
    if (block_residency[block] == null) {
        block_residency[block] = {};
    }
    block_residency[block][nodeID] = true;
  }

  console.log("Block report recieved from: " + req.headers.referer);

  res.send("Block report successfully recieved");
});


// checks on a loop if any Node has not sent a block report in a minute
// in case a node died, it will replicate †he blocks that exist on that node
// ONLY if there exists replicas elsewhere and there are nodes to accomodate
// the new blocks
setInterval(() => {
  for (let node in report) {
    if (Math.abs(report[node].timestamp - Date.now()) > 60000) {
      // if its the only node left, we cant replicate
      if (Object.keys(report).length > 1) {
        // DataNode is dead. Therefore, do:
        // 1- for each block on the deadnode, find a datanode that has that block
        // 2- find a node that doesn't have the block and replicate to it
        // 3- once you are done replicating all blocks, delete the deadnode from the report
        console.log("Dead node : " + node + " has " + report[node].BlockList.length) + " many blocks.";
        for (let i = 0; i < report[node].BlockList.length; i++) {
          let blockID = report[node].BlockList[i];
          console.log("Pottentially Replicating Block: " + blockID);
          console.log("Is there a replica available of the block: " + isThereReplica(blockID));
          console.log("Is there a a node that does not have the block : " + isThereEmptyNodes(blockID));
          if (isThereReplica(blockID) && isThereEmptyNodes(blockID)) {

            let backUp_node = getBackUpNode(node, blockID);
            let next_node = getEmptyNode(blockID);

            console.log("Replicating Block: ");
            console.log(blockID);
            console.log("Report: ");
            console.log(report);
            console.log("Block Residency: ");
            console.log(block_residency);

            // If no empty node exists skip
            if (next_node == null) {
              console.error("No node exists that does not have this block.")
              break;
            }

            const replicateURL =
              report[backUp_node].publicDNS + "/Replicate/" + blockID;
            const NodeURL = report[next_node].publicDNS;
            axios({
              method: "post",
              url: replicateURL,
              data: {
                NodeURL
              }
            })
              .then(function(response) {
                console.log(`Replicated block ${blockID} that died in dead node ${node} from living node ${backUp_node} to living empty node ${next_node}.`)
              })
              .catch(function(error) {
                `Error occured replicating block ${blockID} that died in dead node ${node} from living node ${backUp_node} to living empty node ${next_node}. Status : ${error.response.status}. Message : ${error.response.data}`
              });
          }
        }
      }
      delete report[node];
    }
  }
  // Reset Status for next check:
  resetBlockResidencyStatus();
  for (var nodeID in report) {
    for (var block in report[nodeID].BlockList) {
      let bid = report[nodeID].BlockList[block];
      block_residency[bid][nodeID] = true;
    }
  }
}, CHECK_INTERVAL * 1000);


// Generate unique BlockIDs
function createBlockIDs(blockCount) {
  let blockIDs = [];
  for (var i = 0; i < blockCount; i++) {
    blockIDs.push(current_blockID);
    initializeBlockMetadata(current_blockID);
    current_blockID++;
  }
  return blockIDs;
}


// Initializes the metaData for a unique block
function initializeBlockMetadata(blockID) {
  for (var i = 0; i < START_NODE_COUNT; i++) 
  {
    if (block_residency[blockID] == null) {
      block_residency[blockID] = {}
    }
    block_residency[blockID][i] = false;
  }
}


// Returns true if the node has the block
function hasBlock(nodeID, blockID) {
  console.log("Checking if node " + nodeID + " has block " + blockID);
  return block_residency[blockID][nodeID];
}

function getNodeList(blockIDs, blockCount) {
  let node_list = [];
  let replications = getReplicationCount();
  for (var i = 0; i < blockCount; i++) {
    let list = [];
    var nodeID = getNodeWithLowestBlocks();
    block_counter[nodeID]++;
    var nodeURL = report[nodeID].publicDNS;
    if (block_residency[blockIDs[i]] == null) block_residency[blockIDs[i]] = {};
    block_residency[blockIDs[i]][nodeID] = true;
    list.push(nodeURL);
    for (var replica = 1; replica < replications; replica++) {
      var replica_node = getNodeWithLowestBlocksExcluding(nodeID);
      block_counter[replica_node]++;
      var replica_nodeURL = report[replica_node].publicDNS;
      list.push(replica_nodeURL);
    }
    node_list.push(list);
  }
  return node_list;
}


function getNodeWithLowestBlocks() {
  var nodeID;
  var min = Number.MAX_SAFE_INTEGER;
  for (var node in report) {
    if (block_counter[node] < min) {
      min = block_counter[node];
      nodeID = node;
    }
  }
  return nodeID;
}


function getNodeWithLowestBlocksExcluding(ex_nodeID) {
  var nodeID;
  var min = Number.MAX_SAFE_INTEGER;
  for (var node in report) {
    if (node != ex_nodeID) {
      if (block_counter[node] < min) {
        min = block_counter[node];
        nodeID = node;
      }
    }
  }
  return nodeID;
}


function getEmptyNode(blockID) {
  for (var node in Object.keys(report)) {
    if (!hasBlock(node, blockID)) return node;
  }
  return null;
}


function getBackUpNode(originalNode, blockID) {
  for (var node in block_residency) {
    if (node != originalNode) {
      if (hasBlock(node, blockID)) return node;
    }
  }
}


function getNodeIDWithBlockID(blockID) {
  for (var i = 0; i < START_NODE_COUNT; i++) {
    if (hasBlock(i, blockID)) return i;
  }
}


function getReplicationCount() {
  var activeNodes = getActiveNodes();
  return REPLICATION_FACTOR < activeNodes ? REPLICATION_FACTOR : activeNodes;
}


function getActiveNodes() {
  var count = 0;
  for (var node in report) count++;
  return count;
}


function resetBlockResidencyStatus() {
  for (var block in block_residency) {
    for (var i = 0; i < block_residency[block].length; i++) {
      block_residency[block][i] = false;
    }
  }
}


function isThereReplica(blockID) {
  for (var i = 0; i < START_NODE_COUNT; i++) {
    if (block_residency[blockID][i]) return true;
  }
}


function isThereEmptyNodes(blockID) {
  for (var i = 0; i < START_NODE_COUNT; i++) {
    if (!block_residency[blockID][i]) return true;
  }
}


app.listen(PORT, () => console.log(`NameNode listening on port ${PORT}!`));