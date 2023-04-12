const mysql = require('mysql')

const node1 = mysql.createPool({
    connectionLimit : 100,
    host: '34.92.173.14',
    user: 'root',
    password: 'centralnode',
    database: 'central'
})

const node2 = mysql.createPool({
    connectionLimit : 100,
    host: '34.96.157.106',
    user: 'root',
    password: 'node2',
    database: 'node2'
})

const node3 = mysql.createPool({
    connectionLimit : 100,
    host: '34.92.38.8',
    user: 'root',
    password: 'node3',
    database: 'node3'
})
const nodeChecker = function(offlineNode){
    offlineNode.getConnection((err,connection)=>{
      if(err)
        console.log(err)
      else{
        console.log("Server is back online!")
        //initiate data recovery
        //either use google cloud logs or insert all data from other nodes
        if(offlineNode = "centralNode")
        {

        }
        else if(offline = "node1")
        {

        }
        else if(offline = "node2")
        {
          
        }
      }
    })

  }
  
exports.node1 = node1;
exports.node2 = node2;
exports.node3 = node3;
