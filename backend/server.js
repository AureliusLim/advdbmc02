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

app.get('/centralread', async(req,res)=>{
        connections.node1.getConnection((err,connection)=>{
            if(err){
                
                // Put error message that server is down
                let headerString;
                let header;
                let arr="";
                let arr2="";
                var query = "Select * from before1980";
                var query1 = "Select * from after1980";
                connection.query(query, async(err, result)=>{
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
                //res.sendFile(path.join(__dirname,'views/centralNode.html'));
            }
        })
    
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
    console.log("inside connection");

    connections.node1.getConnection((err,connection)=>{
        if(err){
            //redirect to something else
            console.log(err)
        }
        else{
           
            res.redirect('/centralinsert')
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
   
            var query = "INSERT INTO centraldata (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
            // generate movieid
            var query2 = "SELECT * from centraldata ORDER BY movie_id DESC LIMIT 1;"
            connections.node1.query(query2, (err, result)=>{
                if(err){
                    console.log(err);
                }
                else{
                   
                    console.log(result[0]['movie_id'])
                    generated_id = result[0]['movie_id'] + 1;
                }
            })
            await new Promise(resolve => setTimeout(resolve, 500));
          
            connections.node1.query(query, [generated_id, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                if(err){
                    console.log(err);
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
            
})

app.get('/node2insert', async(req, res)=>{
    
    let movieName = req.cookies["movieName"]
    let movieYear = req.cookies["movieYear"]
    let movieGenre = req.cookies["movieGenre"]
    let director = req.cookies["director"]
    let actor1 = req.cookies["actor1"]
    let actor2 = req.cookies["actor2"]
    var generated_id;
    connections.node2.getConnection(async(err,connection)=>{
        if(err){
            console.log(err);
        }
        else{
            var query = "INSERT INTO before1980 (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
            // generate movieid
            var query2 = "SELECT * from before1980 ORDER BY movie_id DESC LIMIT 1;"
            connections.node2.query(query2, async(err, result)=>{
                if(err){
                    console.log(err);
                }
                else{
                   
                    console.log(result[0]['movie_id'])
                    generated_id = result[0]['movie_id'] + 1;
                }
            })
            await new Promise(resolve => setTimeout(resolve, 500));
          
            connections.node2.query(query, [generated_id, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                if(err){
                    console.log(err);
                }
                else{
                    res.redirect('/editData')
                   
                    console.log(result)
                }
            })
        }
    })
    
})
app.get('/node3insert', async(req, res)=>{
    
    let movieName = req.cookies["movieName"]
    let movieYear = req.cookies["movieYear"]
    let movieGenre = req.cookies["movieGenre"]
    let director = req.cookies["director"]
    let actor1 = req.cookies["actor1"]
    let actor2 = req.cookies["actor2"]
    var generated_id;
    connections.node3.getConnection(async(err,connection)=>{
        if(err){
            console.log(err);
        }
        else{
            var query = "INSERT INTO after1980 (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
            // generate movieid
            var query2 = "SELECT * from after1980 ORDER BY movie_id DESC LIMIT 1;"
            connections.node3.query(query2, async(err, result)=>{
                if(err){
                    console.log(err);
                }
                else{
                   
                    console.log(result[0]['movie_id'])
                    generated_id = result[0]['movie_id'] + 1;
                }
            })
            await new Promise(resolve => setTimeout(resolve, 500));
          
            connections.node3.query(query, [generated_id, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
                if(err){
                    console.log(err);
                }
                else{
                    res.redirect('/editData')
                    console.log(result)
                }
            })
        }
    })
   
})

app.post('/connectionstatusMod', (req, res)=>{
    res.cookie("movieID", req.body.movie_id,{httpOnly:true})
    res.cookie("movieName", req.body.name,{httpOnly:true})
    res.cookie("movieYear", req.body.year, {httpOnly:true})
    res.cookie("movieGenre", req.body.genre, {httpOnly:true})
    res.cookie("director",req.body.director_id, {httpOnly:true})
    res.cookie("actor1", req.body.actor1, {httpOnly:true})
    res.cookie("actor2", req.body.actor2, {httpOnly:true})
    console.log("inside connection");

    connections.node1.getConnection((err,connection)=>{
        if(err){
            //redirect to something else
            console.log(err)

            //call offline checker


            if(Number(movieYear) <= 1980){                    
                res.redirect('/node2modify')
            }
            else{                    
                res.redirect('/node3modify')
            }
        }
        else{
           
            res.redirect('/centralmodify')
        }
    
    })
})

app.get('/centralmodify', async(req, res)=>{
    // let movieName = req.cookies["movieName"]
    // let movieYear = req.cookies["movieYear"]
    // let movieGenre = req.cookies["movieGenre"]
    // let director = req.cookies["director"]
    // let actor1 = req.cookies["actor1"]
    // let actor2 = req.cookies["actor2"]

    // let movieID = req.cookies["movieID"]

    // var movieID = req.cookies["movieID"]
    // var movieName = req.cookies["movieName"]
    // var movieYear = req.cookies["movieYear"]
    // var movieGenre = req.cookies["movieGenre"]
    // var director = req.cookies["director"]
    // var actor1 = req.cookies["actor1"]
    // var actor2 = req.cookies["actor2"]

    var movieID = 2
    var movieName = "$"
    var movieYear = 1971
    var movieGenre = "Comedy-Crime"
    var director = 9970
    var actor1 = 16703
    var actor2 = "33054"

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
    // let movieName = req.cookies["movieName"]
    // let movieYear = req.cookies["movieYear"]
    // let movieGenre = req.cookies["movieGenre"]
    // let director = req.cookies["director"]
    // let actor1 = req.cookies["actor1"]
    // let actor2 = req.cookies["actor2"]

    // let movieID = req.cookies["movieID"]

    // var movieID = req.cookies["movieID"]
    // var movieName = req.cookies["movieName"]
    // var movieYear = req.cookies["movieYear"]
    // var movieGenre = req.cookies["movieGenre"]
    // var director = req.cookies["director"]
    // var actor1 = req.cookies["actor1"]
    // var actor2 = req.cookies["actor2"]

    var movieID = 2
    var movieName = "$"
    var movieYear = 1971
    var movieGenre = "Comedy-Crime"
    var director = 9970
    var actor1 = 16703
    var actor2 = "33054"

    //test if good
        var query = "UPDATE before1980 SET name = '" + movieName + "', year = " + movieYear + ", genre = '" + movieGenre + "', director_id = " + director + ", actor1 = " + actor1 + ", actor2 = " + actor2 + " WHERE movie_id = " + movieID;
        
        // // generate movieid     
        // await new Promise(resolve => setTimeout(resolve, 500));
        
        connections.node2.query(query, [movieID, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
            if(err){
                console.log(err);
            }
            else{
                console.log(result)
                res.redirect('/editData')
            }
        })
})

app.get('/node3modify', async(req, res)=>{
    // let movieName = req.cookies["movieName"]
    // let movieYear = req.cookies["movieYear"]
    // let movieGenre = req.cookies["movieGenre"]
    // let director = req.cookies["director"]
    // let actor1 = req.cookies["actor1"]
    // let actor2 = req.cookies["actor2"]

    // let movieID = req.cookies["movieID"]

    // var movieID = req.cookies["movieID"]
    // var movieName = req.cookies["movieName"]
    // var movieYear = req.cookies["movieYear"]
    // var movieGenre = req.cookies["movieGenre"]
    // var director = req.cookies["director"]
    // var actor1 = req.cookies["actor1"]
    // var actor2 = req.cookies["actor2"]

    var movieID = 2
    var movieName = "$"
    var movieYear = 1971
    var movieGenre = "Comedy-Crime"
    var director = 9970
    var actor1 = 16703
    var actor2 = "33054"

    //test if good
        var query = "UPDATE after1980 SET name = '" + movieName + "', year = " + movieYear + ", genre = '" + movieGenre + "', director_id = " + director + ", actor1 = " + actor1 + ", actor2 = " + actor2 + " WHERE movie_id = " + movieID;
        
        // // generate movieid     
        // await new Promise(resolve => setTimeout(resolve, 500));
        
        connections.node3.query(query, [movieID, movieName, movieYear, movieGenre, director, actor1, actor2], async(err, result)=>{
            if(err){
                console.log(err);
            }
            else{
                console.log(result)
                res.redirect('/editData')
            }
        })
})

app.listen(PORT, ()=>{
    console.log("Server is listening on Port 4000");
});