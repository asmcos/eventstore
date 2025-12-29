// src/db/users.js
const { getClient } = require('./client');
const config = require('../../config/config');
 
const {verifyEvent} = require("eventstore-tools/src/key");

class UserService {
  constructor() {
    this.collections = config.database.collections;
    this.adminPubkey = config.admin.pubkey; // 从配置文件读取管理员公钥
  }

  async getDb() {
    const client = await getClient();
    return client.db(config.database.dbName);
  }

 
  isValidEmail(email) {
    // 邮箱正则表达式 (符合 RFC 5322 标准)
    const emailRegex = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
    
    return emailRegex.test(email);
  }

  // 创建用户（首次建立 pubkey 和 email 的关联，需验证签名）
  async createUser(userData) {
    try {
      const db = await this.getDb();
      const usersCollection = db.collection(this.collections.users);
      const permissionsCollection = db.collection(this.collections.permissions);

 
      // 验证必要字段
      if (!userData.user) {
        return { code: 500, message: '缺少公钥字段' };
      }
      if (!userData.data?.email) {
        return { code: 500, message: '缺少邮箱字段' };
      }
      if (!userData.sig) {
        return { code: 500, message: '缺少签名字段' };
      }
  
      // 验证邮箱格式
      if (!this.isValidEmail(userData.data.email)) {
        return { code: 500, message: '邮箱格式无效' };
      }
  
      // 验证签名
      const isValid = verifyEvent(userData, userData.user);
      if (!isValid) {
        return { code: 500, message: '签名验证失败，公钥与邮箱的绑定关系不可信' };
      }
  
      // 检查公钥是否已存在
      const existingByPubkey = await usersCollection.findOne({ pubkey: userData.user.pubkey });
      if (existingByPubkey) {
        return { code: 501, message: '该公钥已被注册' };
      }
  
      // 检查邮箱是否已存在
      const existingByEmail = await usersCollection.findOne({ email: userData.data.email });
      if (existingByEmail) {
        return { code: 502, message: '该邮箱已被注册' };
      }
  
      // 构建用户文档
      const userDoc = {
        pubkey: userData.user,
        email: userData.data.email,
        createdAt: new Date(),
        updatedAt: new Date()
      };
  
      // 保存用户
      await usersCollection.insertOne(userDoc);

        //add default permissions
      await permissionsCollection.insertOne({
          pubkey: userData.user,
          permissions: config.defaultPermission,
          createdAt: new Date(),
          updatedAt: new Date()
          });
    

      return { code: 200, message: '用户创建成功' };
  
    } catch (error) {
      console.log(error)
      // 处理数据库唯一键冲突（冗余校验，防止并发问题）
      if (error.code === 11000) {
        const conflictField = error.message.includes('pubkey_1') ? '公钥' : '邮箱';
        return { code: 503, message: `该${conflictField}已被注册` };
      }
      // 其他未知错误
      return { code: 599, message: '服务器内部错误' };
    }
  }
  async readUsers(event, limit = 1000,offset = 0) {
    const db = await this.getDb();
    let query = {
    };
    if (event.user) query['pubkey'] = event.user
    if (event.email) query['email'] = event.email

    if (event.offset) offset = event.offset;
    if (event.limit ) limit = event.offset;
    if (event.data && event.data.pubkeys) query['pubkey']  = {$in:event.data.pubkeys}

    return await db.collection(this.collections.users)
      .find(query)
      .sort({ timestamp: -1 })
      .limit(limit)
      .skip(offset)
      .toArray();
  }

  // 根据 pubkey 查找用户
  async getUserByPubkey(pubkey) {
    const db = await this.getDb();
    return db.collection(this.collections.users).findOne({ pubkey });
  }

  // 根据 email 查找用户
  async getUserByEmail(email) {
    const db = await this.getDb();
    return db.collection(this.collections.users).findOne({ email });
  }

  // 更新用户信息（禁止修改 pubkey 和 email）
  async updateUser(pubkey, updates) {
    const db = await this.getDb();

    // 禁止修改 pubkey 和 email（确保关联关系不可变）
    if (updates.pubkey) delete updates.pubkey;
    if (updates.email) delete updates.email;

    // 如果没有可更新的字段，直接返回
    if (Object.keys(updates).length === 0) {
      return await this.getUserByPubkey(pubkey);
    }

    // 更新其他字段
    updates.updatedAt = new Date();

    const result = await db.collection(this.collections.users).findOneAndUpdate(
      { pubkey },
      { $set: updates },
      { returnOriginal: false }
    );

    return result.value;
  }

  
  // 删除用户（逻辑删除）
  async deleteUser(event) {
    const db = await this.getDb();
    	  // 验证event签名（合并双重验证）
    const permissionsCollection = db.collection(this.collections.permissions);  

	  const isValid = verifyEvent(event, this.adminPubkey);
	  if (!isValid) {
      return { code: 500, message: '签名验证失败，公钥与邮箱的绑定关系不可信' };
	  }

    	  // 2. 获取用户当前权限
	  const user = await permissionsCollection.findOne({ pubkey: event.data.pubkey  });
	  
    if (user){
      try {
        const result = await permissionsCollection.deleteOne(
          { pubkey: event.data.pubkey  },
        );
      } catch(e){};
    }
	  


    await db.collection(this.collections.users).deleteOne(
      { pubkey:event.data.pubkey },
       
    );
    return { code: 200, message: '删除用户成功' };
  }

  async counts(){
    const db = await this.getDb();
    const total = await db.collection(this.collections.users).countDocuments();
    return { code: 200, message: '成功', counts:total };
  }
}

module.exports =  UserService;
