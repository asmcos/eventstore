const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');
const config = require('../config/config');

const PORT = 8081;
const STATIC_DIR = path.join(__dirname, '../');// 静态文件目录
// 创建HTTP服务器
const server = http.createServer((req, res) => {
    const parsedUrl = url.parse(req.url);
    const filePath = parsedUrl.pathname === '/' ? 'index.html' : parsedUrl.pathname.slice(1);
  
    // 安全路径处理
    const safePath = path.join(STATIC_DIR, filePath);
    const fullPath = path.normalize(safePath);
  
    // 安全检查 - 防止路径遍历攻击
    if (!fullPath.startsWith(path.join(STATIC_DIR))) {
        res.writeHead(403, { 'Content-Type': 'text/plain' });
        res.end('Forbidden', 'utf-8');
        return;
    }

      
    // 提供文件
    try {
        // 只提供文件服务（不处理目录）
        if (fs.statSync(fullPath).isDirectory()) {
            res.writeHead(403, { 'Content-Type': 'text/plain' });
            res.end('Forbidden: Directory access not allowed', 'utf-8');
            return;
        }

        serveFile(fullPath, res);
    } catch (e) {}
   
});

// 服务单个文件
function serveFile(filePath, res) {
    try {
        const stat = fs.statSync(filePath);
      
        // 设置内容类型
        const contentType = getContentType(filePath);
        res.writeHead(200, { 
            'Content-Type': contentType,
            'Content-Length': stat.size,
            'Cache-Control': 'no-cache'
        });
      
        // 发送文件内容
        const fileStream = fs.createReadStream(filePath);
        fileStream.pipe(res);
      
        // 错误处理
        fileStream.on('error', (err) => {
            console.error('File stream error:', err);
            res.writeHead(500, { 'Content-Type': 'text/plain' });
            res.end('Internal Server Error', 'utf-8');
        });
      
    } catch (err) {
        if (err.code === 'ENOENT') {
            res.writeHead(404, { 'Content-Type': 'text/plain' });
            res.end('File not found', 'utf-8');
        } else {
            res.writeHead(500, { 'Content-Type': 'text/plain' });
            res.end('Internal Server Error', 'utf-8');
        }
        console.error('Error serving file:', err);
    }
}

// 获取内容类型
function getContentType(filePath) {
    const ext = path.extname(filePath).toLowerCase();
    switch (ext) {
        case '.html': return 'text/html; charset=utf-8';
        case '.js': return 'text/javascript; charset=utf-8';
        case '.css': return 'text/css; charset=utf-8';
        case '.json': return 'application/json; charset=utf-8';
        case '.png': return 'image/png';
        case '.jpg': return 'image/jpeg';
        case '.jpeg': return 'image/jpeg';
        case '.gif': return 'image/gif';
        case '.svg': return 'image/svg+xml';
        case '.ico': return 'image/x-icon';
        case '.txt': return 'text/plain; charset=utf-8';
        default: return 'application/octet-stream';
    }
}

// 启动服务器
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Serving files from: ${STATIC_DIR}`);
    console.log('Access files at: http://localhost:' + PORT + '/');
});

// 错误处理
server.on('error', (err) => {
    console.error('Server error:', err);
    process.exit(1);
});
