


//for checking if online
  let intervalCheck;

  function startChecking(offlineNode){

      intervalCheck = setInterval(nodeChecker, 500,offlineNode)
    
  }


  const nodeChecker = function(offlineNode){
    connections.node1.getConnection((err,connection)=>{
      if(err)
        console.log(err)
      else{
        console.log(offlineNode + " is back online!")
        clearInterval(nodeChecker)
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
  
 module.exports = {nodeChecker}


