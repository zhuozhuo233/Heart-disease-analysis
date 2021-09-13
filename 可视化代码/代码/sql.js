//1. 引入express
const {
    response,
    text,
    json
} = require('express');
const express = require('express');
const mysql = require('mysql')

var connection = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'cy0817cy',
    database: 'test'
});

//2. 创建应用对象
const app = express();

//3. 创建路由规则
// request 是对请求报文的封装
// response 是对响应报文的封装
// 如果执行的是/server 那么会执行这个回调函数

connection.connect();

// all接收任意类型
app.all('/json-server', (request, response) => {
    response.setHeader('Access-Control-Allow-Origin', '*');

    var addsql = "select*from information where run = 1";

    connection.query(addsql, function (error, results) {
        if (error) {
            console.log('[select error] - ', error.message);
            return;
        }
        console.log(results);
        // 对对象进行字符串转化
        let str = JSON.stringify(results);
        // 设置响应体
        response.send(str);
    })
})

app.use(express.static('test'));

//4. 监听端口启动服务
app.listen(8001, () => {
    console.log("服务已经启动, 8001 端口监听中....");
});