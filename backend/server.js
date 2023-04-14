const express = require('express');
const bodyParser = require('body-parser');
const connections = require('./connections.js');
const path = require('path');
const PORT = 4000;
const app = express();
const cookieParser = require("cookie-parser")
const fs = require('fs');

app.set('view engine', 'ejs');

app.use(bodyParser.urlencoded({extended:false}));
app.use(bodyParser.json());
app.use(express.static(__dirname));
app.use(cookieParser());

app.get('/',(req,res)=>{
    res.clearCookie("movieName", { httpOnly: true });
    res.clearCookie("movieYear", { httpOnly: true });
    res.clearCookie("movieGenre", { httpOnly: true });
    res.clearCookie("director", { httpOnly: true });
    res.clearCookie("actor1", { httpOnly: true });
    res.clearCookie("actor2", { httpOnly: true });
    res.sendFile(path.join(__dirname,'views/index.html'));
})
app.get('/editData', (req, res)=>{
    res.sendFile(path.join(__dirname,'views/editData.html'))
})

app.get('/centralread', async(req,res)=>{
    let checker = false;
    connections.node2.getConnection((err, connection)=>{
        if(err){
            checker = true;
        }
    })
    connections.node3.getConnection((err, connection)=>{
        if(err){
            checker = true;
        }
    })
    if(checker == true){ // slave nodes are up
        let headerString;
                let header;
                let arr="";
                let arr2="";
                var query = "Select * from before1980";
                var query1 = "Select * from after1980";
                connections.node2.query(query, async(err, result)=>{
                    if(err){

                    }
                    else{
                        header = Object.keys(result[0]);
                        headerString = header.join(",");
                        arr += headerString;
                        arr += "\n";
                        let temp;
                        
                
                        for (let i = 0; i < result.length; i++){
                            temp = Object.values(result[i]);
                            temp[1] = temp[1].replaceAll(',','');
                            temp = temp.toString();
                            temp += ',';
                            arr += temp;
                            arr += "\n";
                            
                        }
                        connections.node3.query(query1, async(err, result)=>{
                            if(err){

                            }
                            else{
                              
                                for (let i = 0; i < result.length; i++){
                                    temp = Object.values(result[i]);
                                    temp[1] = temp[1].replaceAll(',','');
                                    temp = temp.toString();
                                    temp += ',';
                                    arr2 += temp;
                                    arr2 += "\n";
                                    
                                }
                                // sort arr and arr2 when combining the data from node2 and node3
                                let finalarr="";
                                arr = arr.split('\n')
                                arr2 = arr2.split('\n')
                                finalarr += arr[0]
                                finalarr += '\n'
                                let count1 = 1; //skip header
                                let count2 = 0;
                                console.log(arr.length)
                                console.log(arr2.length)
                                while(count1 < arr.length && count2 < arr2.length){
                                    firstpart = arr[count1].split(',');
                                    secondpart = arr2[count2].split(',')
                                    
                                    if(Number(firstpart[0]) < Number(secondpart[0])){ 
                                        finalarr += arr[count1]
                                        finalarr += '\n'
                                        count1++
                                        if(count1 == arr.length){
                                            break;
                                        }
                                    }
                                    else{
                                        finalarr += arr2[count2]
                                        finalarr += '\n'
                                        count2++
                                        if(count2 == arr2.length){
                                            break;
                                        }
                                    }
                                }
        
                                while(count1 < arr.length){
                                    finalarr += arr[count1]
                                    finalarr += '\n'
                                    count1++
                                }
                            
                            
                                while(count2 < arr2.length){
                                    finalarr += arr2[count2]
                                    finalarr += '\n'
                                    count2++
                                }
                               
                              
                                try {
                                    fs.writeFile(path.resolve(__dirname, 'files', "output.csv"), finalarr, async(err)=>{
                                      
                                        if(err){
                                            console.log(err)
                                        }
                                        else{
                                            console.log("saved")
                                            res.download(path.join(__dirname, 'files', "output.csv"))
                                        }
                                    });
                                    // file written successfully
                                  } catch (err) {
                                    console.error(err);
                                  }
                            }
                            
                        })
                    }
                })
    }
    else{
        var query = "Select * from centraldata";
        connections.node1.query(query, async(err, result)=>{
            if(err){
                console.log(err)
            }
            else{
                var header = Object.keys(result[0]);
                headerString = header.join(",");
                var arr="";
                arr += headerString;
                arr += "\n";
                var temp;
                console.log("IN")
        
                for (let i = 0; i < result.length; i++){
                    temp = Object.values(result[i]);
                    temp[1] = temp[1].replaceAll(',','');
                    temp = temp.toString();
                    temp += ',';
                    arr += temp;
                    arr += "\n";
                    //console.log(Object.values(result[i]).join(','))
                }




                try {
                    fs.writeFile(path.resolve(__dirname, 'files', "output.csv"), arr, (err)=>{
                        if(err){
                            console.log(err)
                        }
                        else{
                            console.log("saved")
                            res.download(path.join(__dirname, 'files', "output.csv"))
                        }
                    });
                    // file written successfully
                  } catch (err) {
                    console.error(err);
                  }
            }
            
        })
    }
    
})

// check if good
app.get('/centralreadOne', async(req,res)=>{
    connections.node1.getConnection((err,connection)=>{
        if(err){
            // Put error message that server is down
            let headerString;
            let header;
            let arr="";
            let arr2="";
            let movieID = req.body.movie_id;

            var query = "SELECT * FROM before1980 WHERE movie_id = " + movieID;
            var query1 = "SELECT * FROM after1980 WHERE movie_id = " + movieID;


            connections.node2.query(query, async(err, result)=>{
                if(err){
                    console.log(err)
                }
                else if(result){
                    header = Object.keys(result[0]);
                    headerString = header.join(",");
                    arr += headerString;
                    arr += "\n";
                    let temp;
            
                    // for (let i = 0; i < result.length; i++){
                    temp = Object.values(result[i]);
                    temp[1] = temp[1].replaceAll(',','');
                    temp = temp.toString();
                    temp += ',';
                    arr += temp;
                    arr += "\n";
                }
                else{
                    connections.node3.query(query1, async(err, result)=>{
                        if(err){
                            console.log(err)
                        }
                        else if(result){
                            header = Object.keys(result[0]);
                            headerString = header.join(",");
                            arr += headerString;
                            arr += "\n";
                            let temp;
                    
                            // for (let i = 0; i < result.length; i++){
                            temp = Object.values(result[i]);
                            temp[1] = temp[1].replaceAll(',','');
                            temp = temp.toString();
                            temp += ',';
                            arr += temp;
                            arr += "\n";
                        }
                        else{
                            console.log("Movie Id does not exist")
                        }
                    })             
                }
            })    
                          
            try {
                fs.writeFile(path.resolve(__dirname, 'files', "output.csv"), finalarr, async(err)=>{
                
                    if(err){
                        console.log(err)
                    }
                    else{
                        console.log("saved")
                        res.download(path.join(__dirname, 'files', "output.csv"))
                    }
                });
                // file written successfully
            } catch (err) {
                console.error(err);
                }
        }
        
        else{
            // let movieID = req.body.movie_id
            let movieID = 2

            var query = "SELECT * FROM centraldata WHERE movie_id = " + movieID

            connections.node1.query(query, async(err, result)=>{
                if(err){
                    console.log(err)
                }
                else{
                    var header = Object.keys(result[0]);
                    headerString = header.join(",");
                    var arr="";
                    arr += headerString;
                    arr += "\n";
                    var temp;
                    console.log("IN")
            
                    let i = 0
                    temp = Object.values(result[i]);
                    temp[1] = temp[1].replaceAll(',','');
                    temp = temp.toString();
                    temp += ',';
                    arr += temp;
                    arr += "\n";

                    try {
                        fs.writeFile(path.resolve(__dirname, 'files', "output.csv"), arr, (err)=>{
                            if(err){
                                console.log(err)
                            }
                            else{
                                console.log("saved")
                                res.download(path.join(__dirname, 'files', "output.csv"))
                            }
                        });
                        // file written successfully
                      } catch (err) {
                        console.error(err);
                      }
                }
                
            })
            // res.sendFile(path.join(__dirname,'views/centralNode.html'));
        }
    })
})

app.get('/node2read', async(req,res)=>{
    connections.node2.getConnection((err,connection)=>{
        if(err){
            let headerString;
            let header;
            let arr="";
            var query = "Select * from centraldata where year < 1980";
           console.log("redirected")
            connections.node1.query(query, async(err, result)=>{
                if(err){

                }
                else{
                    header = Object.keys(result[0]);
                    headerString = header.join(",");
                    arr += headerString;
                    arr += "\n";
                    let temp;
                    
            
                    for (let i = 0; i < result.length; i++){
                        temp = Object.values(result[i]);
                        temp[1] = temp[1].replaceAll(',','');
                        temp = temp.toString();
                        temp += ',';
                        arr += temp;
                        arr += "\n";
                        
                    }     
                    try {
                        fs.writeFile(path.resolve(__dirname, 'files', "output.csv"), arr, async(err)=>{
                            
                            if(err){
                                console.log(err)
                            }
                            else{
                                console.log("saved")
                                res.download(path.join(__dirname, 'files', "output.csv"))
                            }
                        });
                        // file written successfully
                        } catch (err) {
                        console.error(err);
                        }
                            
                    
                    }
            })
            
        }
        else{
            var query = "Select * from before1980";
            connections.node2.query(query, async(err, result)=>{
                if(err){
                    console.log(err)
                }
                else{
                   
                    var header = Object.keys(result[0]);
                    headerString = header.join(",");
                    var arr="";
                    arr += headerString;
                    arr += "\n";
                    var temp;
                    console.log("IN")
            
                    for (let i = 0; i < result.length; i++){
                        temp = Object.values(result[i]);
                        temp[1] = temp[1].replaceAll(',','');
                        temp = temp.toString();
                        temp += ',';
                        arr += temp;
                        arr += "\n";
                        
                    }




                    try {
                        fs.writeFile(path.resolve(__dirname, 'files', "output.csv"), arr, (err)=>{
                            if(err){
                                console.log(err)
                            }
                            else{
                                console.log("saved")
                                res.download(path.join(__dirname, 'files', "output.csv"))
                            }
                        });
                        // file written successfully
                      } catch (err) {
                        console.error(err);
                      }

                }
                
            })
            //res.sendFile(path.join(__dirname,'views/node1.html'));
           
        }
    })
   
})
app.get('/node3read', async(req,res)=>{
    connections.node3.getConnection((err,connection)=>{
        if(err){
            let headerString;
            let header;
            let arr="";
            var query = "Select * from centraldata where year >= 1980";
           console.log("redirected")
            connections.node1.query(query, async(err, result)=>{
                if(err){

                }
                else{
                    header = Object.keys(result[0]);
                    headerString = header.join(",");
                    arr += headerString;
                    arr += "\n";
                    let temp;
                    
            
                    for (let i = 0; i < result.length; i++){
                        temp = Object.values(result[i]);
                        temp[1] = temp[1].replaceAll(',','');
                        temp = temp.toString();
                        temp += ',';
                        arr += temp;
                        arr += "\n";
                        
                    }     
                    try {
                        fs.writeFile(path.resolve(__dirname, 'files', "output.csv"), arr, async(err)=>{
                            
                            if(err){
                                console.log(err)
                            }
                            else{
                                console.log("saved")
                                res.download(path.join(__dirname, 'files', "output.csv"))
                            }
                        });
                        // file written successfully
                        } catch (err) {
                        console.error(err);
                        }
                            
                    
                    }
            })
        }
        else{
            var query = "Select * from after1980";
            connections.node3.query(query, async(err, result)=>{
                if(err){
                    console.log(err)
                }
                else{
                    var header = Object.keys(result[0]);
                    headerString = header.join(",");
                    var arr="";
                    arr += headerString;
                    arr += "\n";
                    var temp;
                    console.log("IN")
            
                    for (let i = 0; i < result.length; i++){
                        temp = Object.values(result[i]);
                        temp[1] = temp[1].replaceAll(',','');
                        temp = temp.toString();
                        temp += ',';
                        arr += temp;
                        arr += "\n";
                        //console.log(Object.values(result[i]).join(','))
                    }




                    try {
                        fs.writeFile(path.resolve(__dirname, 'files', "output.csv"), arr, (err)=>{
                            if(err){
                                console.log(err)
                            }
                            else{
                                console.log("saved")
                                res.download(path.join(__dirname, 'files', "output.csv"))
                            }
                        });
                        // file written successfully
                      } catch (err) {
                        console.error(err);
                      }
                }
                
            })
            //res.sendFile(path.join(__dirname,'views/node2.html'));
        }
    })
    
})
app.get('/centraldelete', async(req, res)=>{
    movie = req.body.movie_id;
    //testing
    //movie = '2'
    connections.node1.getConnection((err,connection)=>{
        if(err){
            console.log(err);
        }
        else{
            var query = "DELETE FROM centraldata WHERE movie_id = \'" + movie + "\'";
            connections.node1.query(query, (err, result)=>{
            if(err){
                console.log(err);
            }
            else{
                console.log("Record Deleted: ", result);
            }
        })
        }
        
       
    })
})
app.get('/node2delete', async(req, res)=>{
    movie = req.body.movie_id;
    // testing
    //movie = '5'; 
    connections.node2.getConnection((err,connection)=>{
        if(err){
            console.log(err);
        }
        else{
            var query = "DELETE FROM before1980 WHERE movie_id = \'" + movie + "\'";
            connections.node2.query(query, (err, result)=>{
            if(err){
                console.log(err);
            }
            else{
                console.log("Record Deleted: ", result);
            }
        })
        }
        
       
    })
})
app.get('/node3delete', async(req, res)=>{
    movie = req.body.movie_id;
    // testing
    //movie = '15'; 
    connections.node3.getConnection((err,connection)=>{
        if(err){
            console.log(err);
        }
        else{
            var query = "DELETE FROM after1980 WHERE movie_id = \'" + movie + "\'";
            connections.node3.query(query, (err, result)=>{
            if(err){
                console.log(err);
            }
            else{
                console.log("Record Deleted: ", result);
            }
        })
        }
        
       
    })
})
app.post('/connectionstatus', (req, res)=>{
    res.cookie("movieName", req.body.name,{httpOnly:true})
    res.cookie("movieYear", req.body.year, {httpOnly:true})
    res.cookie("movieGenre", req.body.genre, {httpOnly:true})
    res.cookie("director",req.body.director_id, {httpOnly:true})
    res.cookie("actor1", req.body.actor1, {httpOnly:true})
    res.cookie("actor2", req.body.actor2, {httpOnly:true})
    console.log(req.body.action)
    console.log("inside connection");
    let firstTime = true;
    connections.node1.getConnection((err,connection)=>{
        if(err){
            //check connection of each node
            let movieName = req.body.name
            let movieYear = req.body.year
            let movieGenre = req.body.genre
            let director = req.body.director_id
            let actor1 = req.body.actor1
            let actor2 = req.body.actor2
            let action = req.body.action
            let checkCentral = setInterval(function(){
                connections.node1.getConnection(async(err, connection)=>{
                    if(err){
                        console.log("central is down")
                        // insert to slave logs and write to slave nodes
                        if(firstTime == true){
                            console.log(movieName)
                            
                            connections.node2.getConnection(async(err,connection)=>{
                                if (!err){
                                    console.log("START LOG 2")
                                    var query = "INSERT INTO node2.logs (name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?)";
                                
                                    
                                     connections.node2.query(query,[movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                                            if(err){
                                                //console.log(err);
                                            }
                                            else{                        
                                                console.log("logged in node2")
                                            }
                                        })

                                    
                                }
                            })
                            connections.node3.getConnection(async(err,connection)=>{
                                if(!err){
                                    console.log("START LOG 3")
                                    var query = "INSERT INTO node3.logs (name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?)";
                                
                
                                    connections.node3.query(query,[movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                                            if(err){
                                                console.log(err);
                                            }
                                            else{                        
                                                console.log("logged in node3")
                                            }
                                        })

                                }
                            })
                            if(movieYear<1980){
                                //write node2.before1980
                                let generated_id;
                                let highestnode2;
                                let highestnode3;
                                connections.node2.getConnection(async(err,connection)=>{
                                    if (!err){
                                        var query = "INSERT INTO before1980 (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
                
                                            // generate movieid
                                            var query2 = "SELECT * from before1980 ORDER BY movie_id DESC LIMIT 1;"
                                            connections.node2.query(query2, (err, result)=>{
                                                if(err){
                                                    //console.log(err);
                                                    
                                                }
                                                else{
                                                
                                                    console.log(result[0]['movie_id'])
                                                    highestnode2= result[0]['movie_id'] + 1;
                                                }
                                            })

                                            var query3 = "SELECT * from after1980 ORDER BY movie_id DESC LIMIT 1;"
                                            connections.node3.query(query3, (err, result)=>{
                                                if(err){
                                                    //console.log(err);
                                                    
                                                }
                                                else{
                                                
                                                    console.log(result[0]['movie_id'])
                                                    highestnode3 = result[0]['movie_id'] + 1;
                                                }
                                            })

                                            await new Promise(resolve => setTimeout(resolve, 500));
                                            if(highestnode2 > highestnode3){
                                                generated_id = highestnode2;
                                            }
                                            else{
                                                generated_id = highestnode3;
                                            }
                                            connections.node2.query(query, [generated_id, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                                                if(err){
                                                    //console.log(err);
                                                }
                                                else{
                                                    console.log(result)
                                                }
                                            })
                                        }
                                })
                            }

                            else{
                                var query = "INSERT INTO after1980 (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
                                let generated_id;
                                let highestnode2;
                                let highestnode3;
                                // generate movieid
                                var query2 = "SELECT * from before1980 ORDER BY movie_id DESC LIMIT 1;"
                                connections.node2.query(query2, (err, result)=>{
                                    if(err){
                                        //console.log(err);
                                        
                                        console.log("node2 down")
                                    }
                                    else{
                                        console.log("node2 is up")
                                        console.log(result[0]['movie_id'])
                                       highestnode2 = result[0]['movie_id'] + 1;
                                    }
                                })
                                var query3 = "SELECT * from after1980 ORDER BY movie_id DESC LIMIT 1;"
                                connections.node3.query(query3, (err, result)=>{
                                    if(err){
                                        //console.log(err);
                                        
                                        console.log("node3 down")
                                    }
                                    else{
                                        console.log("node3 is up")
                                        console.log(result[0]['movie_id'])
                                        highestnode3 = result[0]['movie_id'] + 1;
                                    }
                                })
                                await new Promise(resolve => setTimeout(resolve, 500));
                                if(highestnode2 > highestnode3){
                                    generated_id = highestnode2;
                                }
                                else{
                                    generated_id = highestnode3;
                                }
                                connections.node3.query(query, [generated_id, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                                    if(err){
                                        //console.log(err);
                                    }
                                    else{
                                        console.log(result)
                                    }
                                })
                            }

                           
                            firstTime = false;
                        }
                       

                    }
                    else{
                        console.log("Central Server is now up!")
                        var query1 = "SELECT * from centraldata ORDER BY movie_id DESC LIMIT 1;"
            
           
                        connections.node1.query(query1, (err, result)=>{
                            if(err){
                                //console.log(err);
                            }
                            else{
                            
                                console.log(result[0]['movie_id'])
                                generated_id = result[0]['movie_id'] + 1;
                            }
                        })
                        await new Promise(resolve => setTimeout(resolve, 500));
                        connections.node2.getConnection(async(err, connection)=>{
                          if (!err){
                            var resultquery = "SELECT * FROM node2.logs"
                         
                            connections.node2.query(resultquery,(err, node2logs)=>{
                                console.log(node2logs.length)
                                let movieName;
                                let movieYear;
                                let movieGenre;
                                let director;
                                let actor1;
                                let actor2;
                                let temp;
                                for(let index = 0; index < node2logs.length; index++){
                                    temp = Object.values(node2logs[index])
                                    movieName = temp[0];
                                    console.log(movieName);
                                    movieYear = temp[1];
                                    movieGenre = temp[2];
                                    director = temp[3];
                                    actor1 = temp[4];
                                    actor2 = temp[5];
                                    movie_id = temp[6];
                                  
                                    //oppai,  search this
                                    //insert log
                                    if(movieName !== null && movie_id == null)
                                    {
                                        var insertquery = "INSERT INTO centraldata (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
                                
                                        
                                        connections.node1.query(insertquery,[generated_id, movieName, movieYear, movieGenre, director, actor1, actor2], (err, result)=>{
                                               if(err){
                                                   console.log(err);
                                               }
                                               else{                        
                                                   console.log(result)
                                               }
                                           })
                                        generated_id += 1;
                                    }
                                    //modify log
                                    else if(movieName !== null && movie_id !== null)
                                    {
                                        var editquery = "UPDATE centraldata SET name = '" + movieName + "', year = " + movieYear + ", genre = '" + movieGenre + "', director_id = " + director + ", actor1 = " + actor1 + ", actor2 = " + actor2 + " WHERE movie_id = " + movieID;

                                        connections.node1.query(editquery,[movie_id, movieName, movieYear, movieGenre, director, actor1, actor2], (err, result)=>{
                                            if(err){
                                                console.log(err);
                                            }
                                            else{                        
                                                console.log(result)
                                                console.log(result)
                                            }
                                        })
                                    }
                                    
                                    else if(movieName == null && movie_id !== null)
                                    {
                                        var deletequery = "DELETE FROM centraldata WHERE movie_id VALUES(?)";
                                        connections.node1.query(deletequery,[movie_id], (err, result)=>{
                                            if(err){
                                                console.log(err);
                                            }
                                            else{                        
                                                console.log(result)
                                            }
                                        })
                                    }
                                    
                                }
                                var resetQuery = "DELETE FROM node2.logs"
                                connections.node2.query(resetQuery, (err, result)=>{
                                    if (err){
                                        console.log(err)
                                    }
                                    else{
                                        
                                    }
                                })
                                // connections.node1.query('commit;set autocommit = 1', (err, result)=>{
                                //     if(err){
                                //         console.log(err)
                                //     }
                                //     else{
    
                                //     }
                                // })

                            })
                            //delete logs after copying
                           
                          }  
                          else{
                            connections.node3.getConnection(async(err, connection)=>{
                                if (!err){
                                  var query = "SELECT * FROM node3.logs"
      
                                  //get result and copy into the offline node

                                  //delete logs after copying
                                    var resetQuery = "DELETE FROM node3.logs"
                                }  
                              })
                          }
                        })



                        clearInterval(checkCentral)
                    
                    }
                })
            }, 5000)
        
        }
        else{

                        //query for table LOGS and use result for updating the down node

            if(movieYear < 1980)
            {
                connections.node2.getConnection((err,connection)=>{
                if(err){
                    let checkNode2 = setInterval(function(){
                        connections.node2.getConnection(async(err, connection)=>{
                            if(err){
                                console.log("node2 down")
                                if(firstTime == true){
                                    
                                    connections.node1.getConnection(async(err,connection)=>{
                                        if (!err){
                                            console.log("START LOG in Central")
                                            var query = "INSERT INTO central.logs (name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?)";
                                                                        
                                            connections.node1.query(query,[movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                                                if(err){
                                                    console.log(err);
                                                }
                                                else{                        
                                                    console.log("logged in central")
                                                }
                                            })                        
                                        }
                                    })
                                    
                                    firstTime = false;
                                }
                            }
                            else{
                                console.log("node2 is now up")
                                var query1 = "SELECT * from centraldata ORDER BY movie_id DESC LIMIT 1;"
                                let generated_id;
           
                                connections.node1.query(query1, (err, result)=>{
                                    if(err){
                                        console.log(err);
                                    }
                                    else{
                                    
                                        console.log(result[0]['movie_id'])
                                        generated_id = result[0]['movie_id'] + 1;
                                    }
                                })
                                await new Promise(resolve => setTimeout(resolve, 500));
                                connections.node1.getConnection(async(err, connection)=>{
                                    if (!err){
                                        var resultquery = "SELECT * FROM central.logs"
                                    
                                        connections.node1.query(resultquery, (err, node1logs)=>{
                                            console.log(node1logs.length)
                                            let movieName;
                                            let movieYear;
                                            let movieGenre;
                                            let director;
                                            let actor1;
                                            let actor2;
                                            let temp;
                                            for(let index = 0; index < node1logs.length; index++){
                                                temp = Object.values(node1logs[index])
                                                movieName = temp[0];
                                                console.log(movieName);
                                                movieYear = temp[1];
                                                movieGenre = temp[2];
                                                director = temp[3];
                                                actor1 = temp[4];
                                                actor2 = temp[5];
                                                var insertquery = "INSERT INTO before1980 (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
                                            
                                                
                                                connections.node2.query(insertquery,[generated_id + index, movieName, movieYear, movieGenre, director, actor1, actor2], (err, result)=>{
                                                    if(err){
                                                        console.log(err);
                                                    }
                                                    else{                        
                                                        console.log(result)
                                                    }
                                                })
                                            }
                                            var resetQuery = "DELETE FROM central.logs"
                                            connections.node1.query(resetQuery, (err, result)=>{
                                                if (err){
                                                    console.log(err)
                                                }
                                                else{
                                                    
                                                }
                                            })
                        

                                        })
                                
                                    }  
                          
                                })
                                clearInterval(checkNode2)
                            }
                                
                                

                        })
                        //need to prep logs
                    },5000)



                    
                }
               
                })


            }
            // movieYear >= 1980
            else{
                connections.node3.getConnection((err,connection)=>{
                    if(err){
                        let checkNode3 = setInterval(function(){
                            connections.node3.getConnection(async(err, connection)=>{
                                if(err){
                                    console.log("node3 down")
                                    if(firstTime == true){
                                        
                                        connections.node1.getConnection(async(err,connection)=>{
                                            if (!err){
                                                console.log("START LOG in Central")
                                                var query = "INSERT INTO central.logs (name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?)";
                                                                            
                                                connections.node1.query(query,[movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                                                    if(err){
                                                        console.log(err);
                                                    }
                                                    else{                        
                                                        console.log("logged in central")
                                                    }
                                                })                        
                                            }
                                        })
                                        
                                        firstTime = false;
                                    }
                                }
                                else{
                                    console.log("node3 is now up")
                                    var query1 = "SELECT * from centraldata ORDER BY movie_id DESC LIMIT 1;"
                                    let generated_id;
               
                                    connections.node1.query(query1, (err, result)=>{
                                        if(err){
                                            console.log(err);
                                        }
                                        else{
                                        
                                            console.log(result[0]['movie_id'])
                                            generated_id = result[0]['movie_id'] + 1;
                                        }
                                    })
                                    await new Promise(resolve => setTimeout(resolve, 500));
                                    connections.node1.getConnection(async(err, connection)=>{
                                        if (!err){
                                            var resultquery = "SELECT * FROM central.logs"
                                        
                                            connections.node1.query(resultquery, (err, node1logs)=>{
                                                console.log(node1logs.length)
                                                let movieName;
                                                let movieYear;
                                                let movieGenre;
                                                let director;
                                                let actor1;
                                                let actor2;
                                                let temp;
                                                for(let index = 0; index < node1logs.length; index++){
                                                    temp = Object.values(node1logs[index])
                                                    movieName = temp[0];
                                                    console.log(movieName);
                                                    movieYear = temp[1];
                                                    movieGenre = temp[2];
                                                    director = temp[3];
                                                    actor1 = temp[4];
                                                    actor2 = temp[5];
                                                    var insertquery = "INSERT INTO after1980 (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
                                                
                                                    
                                                    connections.node3.query(insertquery,[generated_id + index, movieName, movieYear, movieGenre, director, actor1, actor2], (err, result)=>{
                                                        if(err){
                                                            console.log(err);
                                                        }
                                                        else{                        
                                                            console.log(result)
                                                        }
                                                    })
                                                }
                                                var resetQuery = "DELETE FROM central.logs"
                                                connections.node1.query(resetQuery, (err, result)=>{
                                                    if (err){
                                                        console.log(err)
                                                    }
                                                    else{
                                                        
                                                    }
                                                })
                            
    
                                            })
                                    
                                        }  
                              
                                    })
                                    clearInterval(checkNode3)
                                }
                                    
                                    
    
                            })
                            //need to prep logs
                        },5000)
    
    
    
                        
                    }
                   
                })
            }
            if(req.body.action == "Insert Data"){
                res.redirect('/centralinsert')
            }
            else if(req.body.action == "Modify Data"){
                res.redirect('/')
            }
                
        }
    })
})

app.get('/centralinsert', async(req, res)=>{
    
    
    let movieName = req.cookies["movieName"]
    let movieYear = req.cookies["movieYear"]
    let movieGenre = req.cookies["movieGenre"]
    let director = req.cookies["director"]
    let actor1 = req.cookies["actor1"]
    let actor2 = req.cookies["actor2"]
   
    
    var generated_id;
    connections.node1.getConnection(async(err,connection)=>{
        if(err){

        }
        else{
            var query = "INSERT INTO centraldata (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
            // generate movieid
            var query2 = "SELECT * from centraldata ORDER BY movie_id DESC LIMIT 1;"
            
           
            connections.node1.query(query2, (err, result)=>{
                if(err){
                    //console.log(err);
                }
                else{
                   
                    console.log(result[0]['movie_id'])
                    generated_id = result[0]['movie_id'] + 1;
                }
            })
            //console.log("Delayed")
            await new Promise(resolve => setTimeout(resolve, 500));
            // add delay here so that have enough time to make node go offline
            connections.node1.query(query, [generated_id, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                if(err){
                    console.log(err);
                    //write to logs
                    
                }
                else{
                    console.log(result)
                    if(Number(movieYear) <= 1980){
                        
                        res.redirect('/node2insert')
                        
                    }
                    else{
                       
                        res.redirect('/node3insert')
                    }
                }
            })
        }
    })
            
})

app.get('/node2insert', async(req, res)=>{
    
    let movieName = req.cookies["movieName"]
    let movieYear = req.cookies["movieYear"]
    let movieGenre = req.cookies["movieGenre"]
    let director = req.cookies["director"]
    let actor1 = req.cookies["actor1"]
    let actor2 = req.cookies["actor2"]
    var generated_id;
    let highestnode2;
    let highestnode3;
    let redirect = true;
    let checkNode2 = setInterval(function(){
        connections.node2.getConnection(async(err,connection)=>{
            if(err){
                console.log("node2 is down")
                redirect = false
                //console.log(err);
            }
            else{
                console.log('Node2 is up')
                clearInterval(checkNode2)
                var query = "INSERT INTO before1980 (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
                
                // generate movieid
                var query2 = "SELECT * from before1980 ORDER BY movie_id DESC LIMIT 1;"
                    connections.node2.query(query2, async(err, result)=>{
                        if(err){
                            //console.log(err);
                            redirect = false;
                            console.log("node3 down")
                        }
                        else{
                            console.log("node3 is up")
                            console.log(result[0]['movie_id'])
                            highestnode2 = result[0]['movie_id'] + 1;
                        }
                    })
                    var query3 = "SELECT * from after1980 ORDER BY movie_id DESC LIMIT 1;"
                    connections.node3.query(query3, async(err, result)=>{
                        if(err){
                            //console.log(err);
                            redirect = false;
                            console.log("node3 down")
                        }
                        else{
                            console.log("node3 is up")
                            console.log(result[0]['movie_id'])
                            highestnode3 = result[0]['movie_id'] + 1;
                        }
                    })
                await new Promise(resolve => setTimeout(resolve, 500));
                    if(highestnode2 > highestnode3){
                        generated_id = highestnode2;
                    }
                    else{
                        generated_id = highestnode3;
                    }
                connections.node2.query(query, [generated_id, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                    if(err){
                        //console.log(err);
                    }
                    else{
                        
                        if(redirect == true){
                            res.redirect('/editData')
                        }
                      
                        console.log(result)
                    }
                })
            }
        })}, 5000)
    
})
app.get('/node3insert', async(req, res)=>{
    
    let movieName = req.cookies["movieName"]
    let movieYear = req.cookies["movieYear"]
    let movieGenre = req.cookies["movieGenre"]
    let director = req.cookies["director"]
    let actor1 = req.cookies["actor1"]
    let actor2 = req.cookies["actor2"]
    var generated_id;
    var highestnode2;
    var highestnode3;
    let redirect = true;
    let checkNode3 = setInterval(function(){
            connections.node3.getConnection(async(err,connection)=>{
            if(err){
                console.log('node2 is down');
                redirect = false;
            }
            else{
                console.log('node3 is up')
               
                if (redirect == true){
                    var query = "INSERT INTO after1980 (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
                
                    // generate movieid
                    var query2 = "SELECT * from before1980 ORDER BY movie_id DESC LIMIT 1;"
                    connections.node2.query(query2, async(err, result)=>{
                        if(err){
                            //console.log(err);
                            redirect = false;
                            console.log("node3 down")
                        }
                        else{
                            console.log("node3 is up")
                            console.log(result[0]['movie_id'])
                            highestnode2 = result[0]['movie_id'] + 1;
                        }
                    })
                    var query3 = "SELECT * from after1980 ORDER BY movie_id DESC LIMIT 1;"
                    connections.node3.query(query3, async(err, result)=>{
                        if(err){
                            //console.log(err);
                            redirect = false;
                            console.log("node3 down")
                        }
                        else{
                            console.log("node3 is up")
                            console.log(result[0]['movie_id'])
                            highestnode3 = result[0]['movie_id'] + 1;
                        }
                    })
                    await new Promise(resolve => setTimeout(resolve, 500));
                    if(highestnode2 > highestnode3){
                        generated_id = highestnode2;
                    }
                    else{
                        generated_id = highestnode3
                    }
                    connections.node3.query(query, [generated_id, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                        if(err){
                            //console.log(err);
                        }
                        else{
                            if(redirect == true){
                                res.redirect('/editData')
                            }
                            
                            console.log(result)
                        }
                    })
                }
            }
                
        })
    })
   
})



app.get('/centralmodify', async(req, res)=>{
    let movieID = req.cookies["movieID"]
    let movieName = req.cookies["movieName"]
    let movieYear = req.cookies["movieYear"]
    let movieGenre = req.cookies["movieGenre"]
    let director = req.cookies["director"]
    let actor1 = req.cookies["actor1"]
    let actor2 = req.cookies["actor2"]

  

    //test if good
        var query = "UPDATE centraldata SET name = '" + movieName + "', year = " + movieYear + ", genre = '" + movieGenre + "', director_id = " + director + ", actor1 = " + actor1 + ", actor2 = " + actor2 + " WHERE movie_id = " + movieID;
        
        // // generate movieid     
        // await new Promise(resolve => setTimeout(resolve, 500));
        
        connections.node1.query(query, [movieID, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
            if(err){
                console.log(err);
            }
            else{
                console.log(result)

                //no modify yet for them, copy paste if centralModify is good
                if(Number(movieYear) <= 1980){                    
                    res.redirect('/node2modify')
                }
                else{                    
                    res.redirect('/node3modify')
                }
            }
        })
})

app.get('/node2modify', async(req, res)=>{
    let movieID = req.cookies["movieID"]
    let movieName = req.cookies["movieName"]
    let movieYear = req.cookies["movieYear"]
    let movieGenre = req.cookies["movieGenre"]
    let director = req.cookies["director"]
    let actor1 = req.cookies["actor1"]
    let actor2 = req.cookies["actor2"]
    let redirect = true
    //test if good
        var query = "UPDATE before1980 SET name = '" + movieName + "', year = " + movieYear + ", genre = '" + movieGenre + "', director_id = " + director + ", actor1 = " + actor1 + ", actor2 = " + actor2 + " WHERE movie_id = " + movieID;
        
        // // generate movieid     
        // await new Promise(resolve => setTimeout(resolve, 500));
        connections.node2.getConnection((err,connection)=>{
            if(err){
                console.log("node2 is down");
                redirect = false;
            }
        })
            connections.node2.query(query, [movieID, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                if(err){
                    console.log(err);
                }
                else{
                    console.log(result)
                    if(redirect == true){
                        res.redirect('/editData')
                    }
                    
                }
            })
})

app.get('/node3modify', async(req, res)=>{
    let movieID = req.cookies["movieID"]
    let movieName = req.cookies["movieName"]
    let movieYear = req.cookies["movieYear"]
    let movieGenre = req.cookies["movieGenre"]
    let director = req.cookies["director"]
    let actor1 = req.cookies["actor1"]
    let actor2 = req.cookies["actor2"]
    let redirect = true;
    

    //test if good
        var query = "UPDATE after1980 SET name = '" + movieName + "', year = " + movieYear + ", genre = '" + movieGenre + "', director_id = " + director + ", actor1 = " + actor1 + ", actor2 = " + actor2 + " WHERE movie_id = " + movieID;
        
    
        connections.node3.getConnection((err,connection)=>{
            if(err){
                console.log("node3 is down")
                redirect = false;
            }
            else{
                connections.node3.query(query, [movieID, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                    if(err){
                        console.log(err);
                    }
                    else{
                        console.log(result)
                        if(redirect = true){
                            res.redirect('/editData')
                        }
                      
                    }
                })
            }
        })
            
})

app.listen(PORT, ()=>{
    console.log("Server is listening on Port 4000");
});