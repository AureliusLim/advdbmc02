const express = require('express');
const bodyParser = require('body-parser');
const connections = require('./connections.js');
const path = require('path');
const PORT = 4000;
const app = express();

app.set('view engine', 'ejs');

app.use(bodyParser.urlencoded({extended:false}));
app.use(bodyParser.json());


app.get('/',(req,res)=>{
    res.sendFile(path.join(__dirname,'views/index.html'));
})

// app.get('/centralNode',(req,res)=>{
//     res.sendFile(path.join(__dirname,'views/centralNode.html'));
// })

// app.get('/node1',(req,res)=>{
//     res.sendFile(path.join(__dirname,'views/node1.html'));
// })

// app.get('/node2',(req,res)=>{
//     res.sendFile(path.join(__dirname,'views/node2.html'));
// })

app.get('/editData',(req,res)=>{


    
    //if central node is up
    res.sendFile(path.join(__dirname,'views/editData.html'));

    //else 
})

// app.get('/readData',(req,res)=>{
//     res.sendFile(path.join(__dirname,'views/centralNode.html'));
// })


app.get('/centralread', async(req,res)=>{
    connections.node1.connect((err)=>{
        if(err){
            console.log(err);
        }
        else{
            var query = "Select * from centraldata";
            connections.node1.query(query, async(err, result)=>{
                if(err){
                    console.log(err)
                }
                else{
                    console.log(result)
                }
                
            })
        }
    })
    res.sendFile(path.join(__dirname,'views/centralNode.html'));
})

app.get('/node2read', async(req,res)=>{
    connections.node2.connect((err)=>{
        if(err){
            console.log(err);
        }
        else{
            var query = "Select * from node2";
            connections.node2.query(query, async(err, result)=>{
                if(err){
                    console.log(err)
                }
                else{
                    console.log(result)
                }
                
            })
        }
    })
    res.sendFile(path.join(__dirname,'views/node1.html'));
})
app.get('/node3read', async(req,res)=>{
    connections.node3.connect((err)=>{
        if(err){
            console.log(err);
        }
        else{
            var query = "Select * from node3";
            connections.node3.query(query, async(err, result)=>{
                if(err){
                    console.log(err)
                }
                else{
                    console.log(result)
                }
                
            })
        }
    })
    res.sendFile(path.join(__dirname,'views/node2.html'));
})
app.get('/centraldelete', async(req, res)=>{
    movie = req.body.movie_id;
    //testing
    //movie = '2'
    connections.node1.connect((err)=>{
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
    connections.node2.connect((err)=>{
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
    connections.node3.connect((err)=>{
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

app.get('/centralinsert', async(req, res)=>{
    
    movieName = req.body.name;
    movieYear = req.body.year;
    movieGenre = req.body.genre;
    director = req.body.director_id;
    actor1 = req.body.actor1;
    actor2 = req.body.actor2;

    // testing
    // movieName = "testmovie";
    // movieYear = "2023";
    // movieGenre = "Comedy,Horror";
    // director = "430530459";
    // actor1 = "23904824";
    // actor2 = "23940234";
    var generated_id;
    connections.node1.connect(async(err)=>{
        if(err){
            console.log(err);
        }
        else{
            var query = "INSERT INTO centraldata (movie_id, name, year, genre, director_id, actor1, actor2) VALUES(?,?,?,?,?,?,?)";
            // generate movieid
            var query2 = "SELECT * from centraldata ORDER BY movie_id DESC LIMIT 1;"
            connections.node1.query(query2, async(err, result)=>{
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
                }
            })
        }
    })
})

app.get('/node2insert', async(req, res)=>{
    
    movieName = req.body.name;
    movieYear = req.body.year;
    movieGenre = req.body.genre;
    director = req.body.director_id;
    actor1 = req.body.actor1;
    actor2 = req.body.actor2;

    // testing
    movieName = "testmovie";
    movieYear = "2023";
    movieGenre = "Comedy,Horror";
    director = "430530459";
    actor1 = "23904824";
    actor2 = "23940234";
    var generated_id;
    connections.node2.connect(async(err)=>{
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
                    console.log(result)
                }
            })
        }
    })
})
app.get('/node3insert', async(req, res)=>{
    
    movieName = req.body.name;
    movieYear = req.body.year;
    movieGenre = req.body.genre;
    director = req.body.director_id;
    actor1 = req.body.actor1;
    actor2 = req.body.actor2;

    // testing
    movieName = "testmovie";
    movieYear = "2023";
    movieGenre = "Comedy,Horror";
    director = "430530459";
    actor1 = "23904824";
    actor2 = "23940234";
    var generated_id;
    connections.node3.connect(async(err)=>{
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
                    console.log(result)
                }
            })
        }
    })
})

app.listen(PORT, ()=>{
    console.log("Server is listening on Port 4000");
});