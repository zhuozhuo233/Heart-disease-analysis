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




app.get('/server', (request, response) => {
    // 设置响应头 设置允许跨域
    response.setHeader('Access-Control-Allow-Origin', '*');
    //设置响应
    const data = {
        name: 'cq'
    };

    // 对对象进行字符串转化
    let str1 = JSON.stringify(data);


    response.send(str1);
});

app.post('/server', (request, response) => {
    // 设置响应头 设置允许跨域
    response.setHeader('Access-Control-Allow-Origin', '*');
    //设置响应
    response.send('HELLO AJAX POST');
});

app.get('/ie', (request, response) => {
    // 设置响应头 设置允许跨域
    response.setHeader('Access-Control-Allow-Origin', '*');
    //设置响应
    response.send('HELLO IE -2');
});

// all接收任意类型
app.all('/json-server', (request, response) => {
    response.setHeader('Access-Control-Allow-Origin', '*');

    var addsql = "select*from information where run = 1";

    connection.query(addsql, function (error, results) {
        var rst = results;
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

// 延时响应
app.all('/delay', (request, response) => {
    // 设置响应头 设置允许跨域
    response.setHeader('Access-Control-Allow-Origin', '*');
    response.setHeader('Access-Control-Allow-Headers', '*');
    // 加入定时器
    setTimeout(() => {
        // 延时响应
        response.send('延时响应');
    }, 1000)
});

// jQuery服务
app.all('/jquery-server', (request, response) => {
    // 设置响应头 设置允许跨域
    response.setHeader('Access-Control-Allow-Origin', '*');
    response.setHeader('Access-Control-Allow-Headers', '*');
    const data = {
        name: 'cq'
    };
    response.send(JSON.stringify(data));
});

app.use(express.static('test'));

//4. 监听端口启动服务
app.listen(8001, () => {
    console.log("服务已经启动, 8001 端口监听中....");
});