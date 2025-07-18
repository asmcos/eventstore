// src/server.js
const WebSocket = require('ws');
 
const EventService = require('./db/events');
const UserService = require('./db/users');
const PermissionService = require('./db/permissions');

class WebSocketServer {
  constructor(port = 8080) {
    this.port = port;
    this.wss = null;
    this.eventService = new EventService();
    this.userService = new UserService();
    this.permissionService = new PermissionService();
  }

  // 启动服务器
  async start() {
    try {
      // 初始化WebSocket服务器
      this.wss = new WebSocket.Server({ port: this.port });
      console.log(`WebSocket 服务器启动，监听端口 ${this.port}`);

      // 处理新连接
      this.wss.on('connection', (ws, req) => this.handleConnection(ws, req));

      // 处理服务器错误
      this.wss.on('error', (error) => this.handleServerError(error));
    } catch (error) {
      console.error('启动服务器失败:', error);
      process.exit(1);
    }
  }

  // 处理新客户端连接
  handleConnection(ws, req) {
 

    // 处理接收到的消息
    ws.on('message', (message) => this.handleMessage(message));

    // 处理连接关闭
    ws.on('close', (code, reason) => this.handleDisconnect( code, reason));

    // 处理错误
    ws.on('error', (error) => this.handleClientError(  error));
  }

  // 处理客户端消息
  async handleMessage(  message) {
    try {
      // 解析JSON消息
      message = JSON.parse(message);
      let event = message[2]
      // 验证消息格式
      if (!event.ops || !event.code || !event.user) {
        throw new Error('无效的事件格式: 缺少 ops、code 或 user 字段');
      }

      console.log(` ops=${event.ops}, code=${event.code}, user=${event.user}`);

      let response;
      switch (event.ops) {
        case 'C':
          if (event.code >= 100 && event.code < 200) {
            // 用户相关创建操作
            if (event.code === 100) {
              // 创建用户
              response = await this.userService.createUser({
                pubkey: event.user,
                email: event.data.email,
                sig: event.sig
              });
            }
          } else if (event.code >= 200 && event.code < 300) {
            // 事件相关创建操作
            if (event.code === 200) {
              // 创建事件
              response = await this.eventService.createEvent({
                 
                user: event.user,
                ops: event.ops,
                code: event.code,
                sig: event.sig,
                created_at: event.created_at,
                data: event.data,
                tags: event.tags
              });
            }
          }
          break;
        case 'R':
          if (event.code >= 100 && event.code < 200) {
            // 用户相关读取操作
            if (event.code === 103) {
              // 查询用户信息
              response = await this.userService.getUserByPubkey(event.user);
            }
          } else if (event.code >= 200 && event.code < 300) {
            // 事件相关读取操作
            if (event.code === 203) {
              // 查询事件信息
              response = await this.eventService.readEvents({ user: event.user }, 1000);
            }
          }
          break;
        case 'U':
          if (event.code >= 100 && event.code < 200) {
            // 用户相关更新操作
            if (event.code === 101) {
              // 更新用户信息
              response = await this.userService.updateUser(event.user, event.data);
            }
          } else if (event.code >= 200 && event.code < 300) {
            // 事件相关更新操作
            if (event.code === 201) {
              // 更新事件信息
              // 这里需要在 EventService 中添加更新方法
              throw new Error('更新事件信息的功能暂未实现');
            }
          }
          break;
        case 'D':
          if (event.code >= 100 && event.code < 200) {
            // 用户相关删除操作
            if (event.code === 102) {
              // 删除用户
              response = await this.userService.deleteUser(event.user);
            }
          } else if (event.code >= 200 && event.code < 300) {
            // 事件相关删除操作
            if (event.code === 202) {
              // 删除事件
              // 这里需要在 EventService 中添加删除方法
              throw new Error('删除事件的功能暂未实现');
            }
          }
          break;
        default:
          throw new Error(`未知的 ops 类型: ${event.ops}`);
      }

 
 
    } catch (error) {
      console.error(`处理消息失败 :`, error);

       
    }
  }

  // 处理客户端断开连接
  handleDisconnect(  code, reason) {
 
 
  }

  // 处理客户端错误
  handleClientError(clientId, error) {
    console.error(`客户端错误  :`, error);
  }

  // 处理服务器错误
  handleServerError(error) {
    console.error('WebSocket服务器错误:', error);
  }

  // 关闭服务器
  async close() {
    try {
 
 
      // 关闭WebSocket服务器
      return new Promise((resolve, reject) => {
        if (this.wss) {
          this.wss.close((error) => {
            if (error) {
              console.error('关闭服务器失败:', error);
              reject(error);
            } else {
              console.log('WebSocket服务器已关闭');
              resolve();
            }
          });
        } else {
          resolve();
        }
      });
    } catch (error) {
      console.error('关闭服务器时出错:', error);
      throw error;
    }
  }
}

// 主函数
async function Servermain() {
  try {
    // 创建并启动服务器
    const server = new WebSocketServer(8080);
    await server.start();

    // 优雅处理进程退出
    process.on('SIGINT', async () => {
      console.log('接收到关闭信号，正在优雅退出...');
      await server.close();
      process.exit(0);
    });

    process.on('SIGTERM', async () => {
      console.log('接收到终止信号，正在优雅退出...');
      await server.close();
      process.exit(0);
    });
  } catch (error) {
    console.error('服务器运行失败:', error);
    process.exit(1);
  }
}

// 启动服务器
if (require.main === module) {
  Servermain();
}

// 导出WebSocketServer类，便于测试和扩展
module.exports = {
  WebSocketServer
};
