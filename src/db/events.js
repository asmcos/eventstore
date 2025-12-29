// src/db/events.js
const { getClient } = require('./client');
const config = require('../../config/config');
const {    PERMISSIONS,
    defaultPermissionConfigs
} = require("eventstore-tools/src/common"); 

const {verifyEvent} = require("eventstore-tools/src/key");

class EventService {
  constructor() {
    this.collections = config.database.collections;
    this.adminPubkey = config.admin.pubkey; // 从配置文件读取管理员公钥
    
  }

  // 获取数据库实例（使用共享客户端）
  async getDb() {
    const client = await getClient();
    return client.db(config.database.dbName);
  }

  // 创建事件
  async createEvent(event) {
    const db = await this.getDb();
    const eventsCollection = db.collection(this.collections.events);
    const permissionsCollection = db.collection(this.collections.permissions);

 
    // 时间校验
    const clientTime = new Date(event.created_at);
    const timeDiff = Math.abs(Math.floor(Date.now() / 1000) - clientTime);
    if (timeDiff > 5 * 60 * 1000) { // 5分钟容忍度
      return {code:500,message:'时间和服务器差距太大'}
    }

    // 用户校验
    const user = await permissionsCollection.findOne({ pubkey: event.user });
    if (!user) {
      return {code:500,message:'无效用户'}
    }

    if (event.user != this.adminPubkey && !(user.permissions &PERMISSIONS.CREATE_EVENTS)){
    	return {code:500,message:'无create event权限'};
    }
    
    // 签名校验
   const isValid = verifyEvent(event, event.user);

    if (!isValid) {
      return {code:500,message:'签名验证失败'}
    }

    // 检查是否包含'd'标签，如果有则删除相同user和d值的旧事件
    if (event.tags && Array.isArray(event.tags)) {
      // 查找tags中键为'd'的标签值
      const dTag = event.tags.find(tag => Array.isArray(tag) && tag[0] === 'd');
      
      if (dTag && dTag[1]) { // 确保'd'标签有值
        const dValue = dTag[1];
        // 删除相同user和d值的事件
        const existingCount = await eventsCollection.countDocuments({
          user: event.user,
          tags: { $elemMatch: { $eq: ['d', dValue] } }
        });
        
        if (existingCount > 0) {
          // 标记旧事件为非最新
          const updateResult = await eventsCollection.updateMany(
            {
              user: event.user,
              tags: { $elemMatch: { $eq: ['d', dValue] } },
               
            },
            { $set: { is_latest: false } }
          );
        
        // 可以在这里添加日志，记录覆盖情况
          if (updateResult.modifiedCount > 0) {
            console.log(`标记了 ${updateResult.modifiedCount} 条旧事件为非最新`);
          }
        }
      }
    }
    // 构建事件文档
    event.servertimestamp = new Date();
 
    // 保存事件
    await eventsCollection.insertOne(event);
    return  {code:200,message:'事件创建成功'};
  }

  // 读取事件
  async readEvents(event, limit = 1000, code = 200, status = 1, offset = 0) {
      const db = await this.getDb();
      const query = {};
    
      // 处理查询条件
      if (event.tags) query["tags"] = { $all: event.tags };
      if (event.eventuser) query['user'] = event.eventuser;
      if (event.eventid) query['id'] = event.eventid;
      if (event.data && event.data.pubkeys) query['user']  = {$in:event.data.pubkeys}
      
      // 处理限制条件
      if (event.limit) limit = event.limit;
      // 处理偏移量，确保是正数
      if (event.offset) offset = Math.max(0, parseInt(event.offset, 10) || 0);
      
      // 处理事件代码查询条件
      if (event.hasOwnProperty('eventcode')) {
          if (event.eventcode !== 0)
              query.code = event.eventcode;
      } else {
          query.code = code;
      }
      
      // 处理状态查询条件（管理员权限验证）
      if (event.hasOwnProperty('status')) {
          if (event.status === 0 || event.status === 1) {
              const isValid = verifyEvent(event, this.adminPubkey);
              if (!isValid) {
                  return { code: 500, message: '管理员签名验证失败' };
              }
          }
      }
      
      // 应用状态过滤条件
      if (event.hasOwnProperty('status')) {
          if (event.status !== 0)
              query.status = event.status;
      } else {
          query.status = { $ne: status };
      }
 
      if (!event.showLogs) {
        query.$or = [
          { is_latest: { $exists: false } }, // 没有该字段的老事件或普通事件
          { is_latest: true }                // 仅显示最新的
        ];
      }

      // 执行带偏移量和限制的查询
      return await db.collection(this.collections.events)
          .find(query)
          .sort({ _id: -1 })
          .skip(offset)  // 增加偏移量
          .limit(limit)
          .toArray();
  }


  async deleteEvent(event){

    const db = await this.getDb();
    const eventsCollection = db.collection(this.collections.events);

    let eventid = event.data.eventid 
     
    
    let eventdoc = await eventsCollection.findOne({id:eventid})

    if(!eventdoc) return {code:500,message:"事件不存在"};

    if (event.user != this.adminPubkey && event.user != eventdoc.user)
      return {code:403,message:"你没有删除权限"};

    const isValid = verifyEvent(event, event.user);

    if (!isValid) {
        return {code:500,message:'签名验证失败'}
    }

    if (event.user == eventdoc.user){
      await eventsCollection.deleteOne({id:eventid});
    } 
    if (event.user == this.adminPubkey){
      await eventsCollection.updateOne(
        { id: eventid },
        {
          $set: { 
            status: 1,
            updatedAt: new Date() 
          }
        }
      );
    }
    return  {code:200,message:'事件删除成功'};

  }

  async counts(event){
    const db = await this.getDb();
    let filter = {};
    if (event.eventuser) filter.user = event.eventuser;
    if (event.tags) filter.tags = { $all: event.tags };
  
    if (!event.showLogs) {
      filter.$or = [
        { is_latest: { $exists: false } }, // 没有该字段的老事件或普通事件
        { is_latest: true }                // 仅显示最新的
      ];
    }

    const total = await db.collection(this.collections.events).countDocuments(filter);
    return { code: 200, message: '成功', counts:total };
  }
}

// 导出单例服务
module.exports = EventService ;
