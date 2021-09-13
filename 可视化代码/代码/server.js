// var http = require("http");
// var server = http.createServer(function (req, res) {
//     if (req.url !== "/favicon.ico") {
//         if (res.headersSent)
//             console.log("响应头已发送");
//         else
//             console.log("响应头未发送");
//         res.writeHead(200, {
//             "Content-Type": "text/plain",
//             "Access-Control-Allow-Origin": "http://localhost:8080/"
//         });
//         if (res.headersSent)
//             console.log("响应头已发送");
//         else
//             console.log("响应头未发送");
//         res.write("你好啊!");
//     }
//     res.end();
// });
// server.listen(1337, "localhost", function () {
//     console.log("开始监听...");
// });


const mysql = require("mysql")
const express = require("express")
const {resolveAny} = require("dns")
const bodyParser = require("body-parser")
const app = express()

app.use(bodyParser.urlencoded({extended:false}))
app.use(express.static("./public"))

const db = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'cy0817cy',
    database: 'test'
})
db.connect()

app.get("/users", (req, res) => {
    db.query("select *from information where run=1",(err,user)=>{
        if(err){
            throw err
        }else{
            var data = JSON.stringify(user)
            res.send(data)
            console.log(data)
        }
    })

})


app.delete("/users", (req, res) => {

})

app.put("/users", (req, res) => {

})


app.listen(8000, () => {
    console.log("服务启动");
})