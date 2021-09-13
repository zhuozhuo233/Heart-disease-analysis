const mysql = require('mysql');
const express = require('express');
const app = express();
var connection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'cy0817cy',
  database: 'test'
});

connection.connect();



// app.use(express.static(path.join(__dirname, 'static'))); //托管html css js  等静态资源


var addsql = 'select*from user where id = 2';
// var addSqlParams = ['cy','123456','cq','12345678910'];



app.all('/json-server', (request, response) => {
  response.setHeader('Access-Control-Allow-Origin', '*');

  connection.query(addsql, function (error, results) {
    if (error) {
      console.long('[select error] - ', error.message);
      return;
    }

    // const data = results;

    // 响应一个对象
    const data = {
      name: 'cq'
    };

    // 对对象进行字符串转化
    let str = JSON.stringify(data);
    设置响应体
    response.send(str);
  })


})

app.listen(8001, () => {
  console.log("服务已经启动, 8001 端口监听中....");
});