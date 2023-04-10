const mysql = require('mysql')

const node1 = mysql.createPool({
    connectionLimit : 100,
    host: '34.92.14.182',
    user: 'root',
    password: 'centralnode',
    database: 'central'
})

const node2 = mysql.createPool({
    connectionLimit : 100,
    host: '34.92.141.140',
    user: 'root',
    password: 'node2',
    database: 'node2'
})

const node3 = mysql.createPool({
    connectionLimit : 100,
    host: '34.92.211.126',
    user: 'root',
    password: 'node3',
    database: 'node3'
})

exports.node1 = node1;
exports.node2 = node2;
exports.node3 = node3;