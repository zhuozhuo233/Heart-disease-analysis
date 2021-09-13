const express = require('express');

const path = require('path');

const app = express();

app.use(express.static(path.join(__dirname,'join')));

app.get('/first',(req,res)=>{

    res.send('hello ajax');

})

app.get('/responDate',(req,res)=>{
    res.send({"name":"zhangsn"})
})

app.listen(3001);

console.log('服务启动!');