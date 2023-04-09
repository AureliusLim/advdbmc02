
function writeToFile(path, binary, object){
    print( "Writing " + path + " to file..." );
    var output = writeFile( path, mode, object );
    print( "The number of bytes written to file was: " + output );
    return output
}
function before1980(){
  $.get('/node2read', {}, (result) => {
    //console.log(Object.keys(result[0]));
    
    var header = Object.keys(result[0]);
    headerString = header.join(",");
    var arr="";
    for (let i = 0; i < result.length; i++){
        arr += Object.values(result[i]).join(',');
        arr += "\n";
        //console.log(Object.values(result[i]).join(','))
    }
    var filePath = "../files/output.csv";
    var isMode = null;
    var fileObject = arr;
    writeToFile(filePath,isMode,fileObject)
  })
}

