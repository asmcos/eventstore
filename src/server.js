const WebSocket = require('ws');
const EventService = require('./db/events');
const UserService = require('./db/users');
const PermissionService = require('./db/permissions');
const config = require('../config/config');
const path = require('path');
const fs = require('fs').promises;
const {
  PERMISSIONS,
  defaultPermissionConfigs
} = require("eventstore-tools/src/common");

const { verifyEvent } = require("eventstore-tools/src/key");

class WebSocketServer {
  constructor(port = 8080) {
    this.port = port;
    this.wss = null;
    this.subscriptions = {};
    this.nextSubscriptionId = 1;
    this.eventService = new EventService();
    this.userService = new UserService();
    this.permissionService = new PermissionService();
    this.uploadDir = path.join(__dirname, '../', config.uploaddir || 'uploads');
    this.ensureUploadDirExists();
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
    ws.on('message', (message) => this.handleMessage(ws, message));

    // 处理连接关闭
    ws.on('close', (code, reason) => this.handleDisconnect(code, reason));

    // 处理错误
    ws.on('error', (error) => this.handleClientError(error));
  }

  // 处理客户端消息
  async handleMessage(ws, message) {
    try {
      // 解析JSON消息
      const parsedMessage = JSON.parse(message);
      const event = parsedMessage[2];

      // 验证消息格式
      if (parsedMessage[0] === "UNSUB") {
        for (const subscriptionId in this.subscriptions) {
          if (this.subscriptions[subscriptionId].clientid === parsedMessage[1]) {
            delete this.subscriptions[subscriptionId];
          }
        }
        return;
      }

      if (!event.ops || !event.code) {
        throw new Error('无效的事件格式: 缺少 ops、code 字段');
      }

      console.log(`ops=${event.ops}, code=${event.code}`);

      let response;
      switch (event.ops) {
        case 'C':
          if (event.code >= 100 && event.code < 200) {
            // 用户相关创建操作
            if (event.code === 100) {
              // 创建用户
              response = await this.userService.createUser(event);
            }
          } else if (event.code >= 200 && event.code < 300) {
            // 事件相关创建操作
            if (event.code === 200) {
              // 创建事件
              response = await this.eventService.createEvent(event);
              if (response) {
                this.matchSubscriptions(event);
              }
            }
          } else if (event.code >= 300 && event.code < 400) {
            if (event.code === 300) {
              response = await this.permissionService.assignPermission(event);
            }
          } else if (event.code >= 400 && event.code < 500) {
            if (event.code === 400) {
              response = await this.handleFileUpload(ws, parsedMessage);
            }
          }
          break;
        case 'R':
          if (event.code >= 100 && event.code < 200) {
            // 用户相关读取操作
            if (event.code === 103) {
              // 查询用户信息
              let filter = {};
              let limit = 1000;
              if (event.limit) limit = event.limit;
              if (event.data) filter = event.data;

              response = await this.userService.readUsers(filter, limit);
              
              this.handleResp(ws, parsedMessage[1], response);
            }
          } else if (event.code >= 200 && event.code < 300) {
            // 事件相关读取操作
            if (event.code === 203) {
              // 查询事件信息
              let filter = {};
              let limit = 1000;
              if (event.limit) limit = event.limit;
              if (event.tags) filter['tags'] = event.tags;
              if (event.user) filter['user'] = event.user;

              this.subscriptions[this.getNextSubscriptionId()] = {
                clientid: parsedMessage[1],
                ws: ws,
                event: event,
              };

              response = await this.eventService.readEvents(filter, limit);
              this.handleResp(ws, parsedMessage[1], response);
            }
          } else if (event.code >= 300 && event.code < 400) {
            // 权限相关读取操作
            if (event.code === 303) {
              // 查询权限信息
              let filter = {};
              let limit = 1000;
              if (event.limit) limit = event.limit;
              if (event.data) filter = event.data;

              response = await this.permissionService.readPermissions(filter, limit);
              
              this.handleResp(ws, parsedMessage[1], response);
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

  handleResp(ws, messageId, response) {
    if (Array.isArray(response)) {
      // 2. 非空结果：for 循环逐条发送
      for (const item of response) {
        ws.send(JSON.stringify(["RESP", messageId, item]));
      }
    } else {
      ws.send(JSON.stringify(["RESP", messageId, response]));
    }
    ws.send(JSON.stringify(["RESP", messageId, "EOSE"]));
  }

  // 处理客户端断开连接
  handleDisconnect(code, reason) {
    for (const subscriptionId in this.subscriptions) {
      if (this.subscriptions[subscriptionId].ws.readyState === WebSocket.CLOSED) {
        delete this.subscriptions[subscriptionId];
      }
    }
  }

  // 处理客户端错误
  handleClientError(clientId, error) {
    console.error(`客户端错误 :`, error);
  }

  // 处理服务器错误
  handleServerError(error) {
    console.error('WebSocket服务器错误:', error);
  }

  getNextSubscriptionId() {
    return this.nextSubscriptionId++;
  }

  // 匹配订阅并发送消息给订阅者
  matchSubscriptions(event) {
    for (const subscriptionId in this.subscriptions) {
      const subscription = this.subscriptions[subscriptionId];
      const subscriptionEvent = subscription.event;
      const ws = subscription.ws;

      // 订阅了某个用户的
      if (subscriptionEvent.user && subscriptionEvent.user === event.user && !subscriptionEvent.tags) {
        ws.send(JSON.stringify(["RESP", subscription.clientid, event]));
        continue;
      }

      // 带有 tags 的订阅
      if (subscriptionEvent.tags && subscriptionEvent.tags.length > 0) {
        // 检查 event.tags 是否包含所有 subscriptionEvent.tags
        const isSubset = subscriptionEvent.tags.every(tag =>
          event.tags.some(eventTag => eventTag[0] === tag[0] && eventTag[1] === tag[1])
        );

        if (isSubset) {
          ws.send(JSON.stringify(["RESP", subscription.clientid, event]));
          continue;
        }
      }

      // 同时指定 tags 和 user
      if (subscriptionEvent.user && subscriptionEvent.tags && subscriptionEvent.tags.length > 0) {
        // 检查 event.tags 是否包含所有 subscriptionEvent.tags
        const isSubset = subscriptionEvent.tags.every(tag =>
          event.tags.some(eventTag => eventTag[0] === tag[0] && eventTag[1] === tag[1])
        );

        if (isSubset && subscriptionEvent.user === event.user) {
          ws.send(JSON.stringify(["RESP", subscription.clientid, event]));
          continue;
        }
      }

      // 全部订阅
      if (!subscriptionEvent.tags && !subscriptionEvent.user) {
        ws.send(JSON.stringify(["RESP", subscription.clientid, event]));
        continue;
      }
    }
  }

  async handleFileUpload(ws, message) {
    let event = message[2];
    try {
      // 验证用户权限
      const hasPermission = await this.permissionService.hasPermission(
        event.user,
        PERMISSIONS.UPLOAD_FILES
      );

      if (!hasPermission) {
        ws.send(JSON.stringify(["RESP", message[1], { msg: "没有权限", code: 403 }]));
        return;
      }

      const fileData = event.data.fileData;
      delete event.data.fileData;

      const isValid = verifyEvent(event, event.user);
      if (!isValid) {
        ws.send(JSON.stringify(["RESP", message[1], { msg: "签名验证失败", code: 403 }]));
        throw new Error('签名验证失败');
      }

      // 生成唯一文件名
      const fileName = `${event.id}-${event.data.fileName}`;
      const filePath = path.join(this.uploadDir, fileName);

      // 写入文件
      await fs.writeFile(filePath, Buffer.from(fileData.data));

      let response = await this.eventService.createEvent(event);

      // 返回成功响应
      ws.send(JSON.stringify(["RESP", message[1], {
        type: 'SUCCESS',
        code: 200,
        message: '文件上传成功',
        fileUrl: `${fileName}`
      }]));

    } catch (error) {
      console.error('文件上传失败:', error);
      ws.send(JSON.stringify(["RESP", message[1], {
        type: 'ERROR',
        code: 500,
        message: '文件上传失败: ' + error.message
      }]));
    }
  }

  // 确保 uploadDir 存在，如果不存在则创建
  async ensureUploadDirExists() {
    try {
      await fs.access(this.uploadDir, fs.constants.F_OK);
    } catch (error) {
      if (error.code === 'ENOENT') {
        // 目录不存在，创建目录
        await fs.mkdir(this.uploadDir, { recursive: true });
        console.log(`上传目录已创建: ${this.uploadDir}`);
      } else {
        throw error;
      }
    }
  }

  // 关闭服务器
  async close() {
    try {
      // 关闭WebSocket服务器
      return new Promise((resolve, reject) => {
        setTimeout(() => {
          reject(new Error("close WebSocket timeout")); // 建议用 Error 对象，便于捕获堆栈
        }, 5000);

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
      console.log('exit');
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
