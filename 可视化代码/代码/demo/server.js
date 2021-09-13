const express = require('express');

const app = express();

app.use(express.static('static'));

//这个是提供图表数据的接口
app.get('/data',(request,Response)=>{
    let data = {
        data:[820, 932, 901, 934, 1290, 1330, 1320]
    }
    Response.send(data)
})

app.listen(8001,()=>{
    console.log('服务已启动，浏览器打开http://127.0.0.1:8001查看效果')
})