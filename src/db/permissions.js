const { getClient } = require('./client');
const config = require('../../config/config');

const userService = require('./users'); // 引入用户服务
const logger = require('../utils/logger'); // 引入日志模块 
const {    PERMISSIONS,
    defaultPermissionConfigs
} = require("eventstore-tools/src/common");

const {verifyEvent} = require("eventstore-tools/src/key");

class PermissionService {
  constructor() {
    this.collections = config.database.collections;
    
    this.adminPubkey = config.admin.pubkey; // 从配置文件读取管理员公钥
  }

  async getDb() {
    const client = await getClient();
    return client.db(config.database.dbName);
  }

  

  // 初始化默认权限配置（首次启动时调用）
  async initDefaultPermissions() {
    const db = await this.getDb();
     
    const permissionsCollection = db.collection(this.collections.permissions);
    // 验证管理员签名（防止非管理员初始化）
    const initData = { action: 'init_default_permissions', timestamp: new Date().toISOString() };
 
    // 检查是否已初始化（通过检查管理员是否已有权限值）
    const adminUser = await permissionsCollection.findOne({ pubkey: this.adminPubkey });
    if (adminUser && adminUser.permissions !== undefined) {
      logger.info('默认权限已初始化，无需重复操作');
      throw new Error('默认权限已初始化，无需重复操作');
    }



    try {
      // 更新管理员用户，设置为admin权限
      await permissionsCollection.insertOne(
        { pubkey: this.adminPubkey ,
         permissions: defaultPermissionConfigs.admin, 
        updatedAt: new Date() } 
      );
      
      logger.info('默认权限初始化成功');
      return defaultPermissionConfigs;
    } catch (error) {
      logger.error('默认权限初始化失败:', error);
      throw new Error('默认权限初始化失败');
    }
  }

 
	// 从event参数中提取数据并验证管理员签名
	async assignPermission(event) {
	  const db = await this.getDb();
	  const permissionsCollection = db.collection(this.collections.permissions);
	  
	  // 从event.data中提取userId和permissionName
	  const { userId, permissionValue } = event.data;
	  
	  // 验证event签名（合并双重验证）
	  const isValid = verifyEvent(event, this.adminPubkey);
	  if (!isValid) {
	     return { code: 500, message:  '签名验证失败'};
	  }

 

	  // 2. 获取用户当前权限（不存在则创建）
	  const user = await permissionsCollection.findOne({ pubkey: userId });
	  
	  // 3. 处理权限逻辑（创建或更新）
	  let newPermissions;
	  if (!user) {
	    // 用户不存在：创建新记录并赋予初始权限
	    newPermissions = permissionValue;
	    try {
	      await permissionsCollection.insertOne({
          pubkey: userId,
          permissions: newPermissions,
          createdAt: new Date(),
          updatedAt: new Date()
	      });
	      logger.info(`成功为新用户 ${userId} 创建并分配权限 ${permissionValue}`);
	      return { code: 200, message:  '权限分配成功'};
	    } catch (error) {
	      logger.error(`创建用户 ${userId} 权限记录失败:`, error);
	       return { code: 500, message:  '权限分配失败'};
	    }
	  } else {
	    // 用户存在：更新现有权限
	    newPermissions =   permissionValue;
	    try {
	      const result = await permissionsCollection.updateOne(
          { pubkey: userId },
          {
            $set: { 
              permissions: newPermissions,
              updatedAt: new Date() 
            }
          }
	      );
	      
	      if (result.modifiedCount === 0 && user.permissions === newPermissions) {
		      logger.info(`用户 ${userId} 已拥有权限 ${permissionName}`);
	      } else {
          // 优化后代码

	        logger.info(`成功为用户 ${userId} 更新并分配权限 ${permissionValue}`);
	      }
	      return { code: 200, message:  '权限更新成功'};
	    } catch (error) {
	        logger.error(`为用户 ${userId} 分配权限 ${permissionName} 失败:`, error);
	       return { code: 500, message:  '权限更新失败'};
	    }
	  }
	}

	// 撤销用户权限（从event参数中提取数据）
	async revokePermission(event) {
	  const db = await this.getDb();
	  const permissionsCollection = db.collection(this.collections.permissions);
	  
	  // 从event.data中提取userId和permissionName
	  const { userId, permissionValue } = event.data;
	  
	  // 验证event签名
	  const isValid = verifyEvent(event, this.adminPubkey);
	  if (!isValid) {
	    throw new Error('签名验证失败 ');
	  }

 

	  // 2. 获取用户当前权限
	  const user = await permissionsCollection.findOne({ pubkey: userId });
	  
	  if (!user || !user.permissions) {
	    logger.info(`用户 ${userId} 没有任何权限可撤销`);
	    return { modifiedCount: 0 };
	  }
	  
	  // 3. 撤销权限（使用位运算AND NOT）
	  
	  try {
	    const result = await permissionsCollection.deleteOne(
	      { pubkey: userId },
	       
	    );
	    
	    if (result.modifiedCount === 0 ) {
	      logger.info(`用户 ${userId} 未拥有权限 ${permissionName}`);
	    } else {
	      logger.info(`成功撤销用户 ${userId} 的权限 ${permissionName}`);
	    }
	    return result;
	  } catch (error) {
	    logger.error(`撤销用户 ${userId} 的权限 ${permissionName} 失败:`, error);
	    throw new Error(`撤销用户 ${userId} 的权限 ${permissionName} 失败`);
	  }
	}

  async readPermissions(event, limit = 1000,offset=0) {
    const db = await this.getDb();
    let query = {
            };
    // 处理限制条件
    if (event.limit) limit = event.limit;
    // 处理偏移量，确保是正数
    if (event.offset) offset = Math.max(0, parseInt(event.offset, 10) || 0); 
    
    if (event.data.pubkeys) query['pubkey']  = {$in:event.data.pubkeys}
    
    return await db.collection(this.collections.permissions)
      .find(query)
      .sort({ timestamp: -1 })
      .limit(limit)
      .skip(offset)
      .toArray();
  }
  
  // 获取用户所有权限（返回权限名称列表）
  async getUserPermissions(userId) {
    const db = await this.getDb();
    const permissionsCollection = db.collection(this.collections.permissions);

    // 1. 获取用户信息
    const user = await permissionsCollection.findOne(
      { pubkey: userId },
       
    );
    if (!user) {
      logger.error(`用户 ${userId} 不存在`);
      throw new Error(`用户 ${userId} 不存在`);
    }

    // 2. 解析权限位值为权限名称列表
    const permissionValue = user.permissions || 0;
    const permissionNames = [];
    
    if (permissionValue & PERMISSIONS.MANAGE_USERS) {
      permissionNames.push('user:manage');
    }
    if (permissionValue & PERMISSIONS.MANAGE_PERMISSIONS) {
      permissionNames.push('permission:manage');
    }
    if (permissionValue & PERMISSIONS.MANAGE_EVENTS) {
      permissionNames.push('event:manage');
    }
    if (permissionValue & PERMISSIONS.CREATE_EVENTS) {
      permissionNames.push('event:create');
    }
    if (permissionValue & PERMISSIONS.READ_OWN_EVENTS) {
      permissionNames.push('event:read:own');
    }
    if (permissionValue & PERMISSIONS.READ_PUBLIC_EVENTS) {
      permissionNames.push('event:read:public');
    }
    
    logger.info(`成功获取用户 ${userId} 的权限: ${permissionNames.join(', ')}`);
    return permissionNames;
  }

  // 检查用户是否有特定权限
  async hasPermission(userId, permissionValue) {
    try {
      const db = await this.getDb();
      const permissionsCollection = db.collection(this.collections.permissions);
      
      // 获取用户权限值
 
      const user = await permissionsCollection.findOne(
        { pubkey: userId },
         
      );
   
      if (!user) {
        throw new Error(`用户 ${userId} 不存在`);
      }
      
      // 将权限名称转换为位值
      const requiredPermission =  permissionValue;
      
      // 检查权限
      const hasPerm = user.permissions & requiredPermission;
      logger.info(`用户 ${userId} 拥有权限 ${permissionValue}: ${!!hasPerm}`);
      return !!hasPerm;
    } catch (error) {
      logger.error(`检查用户 ${userId} 是否拥有权限 ${permissionValue} 失败:`, error);
      throw new Error(`检查用户 ${userId} 是否拥有权限 ${permissionValue} 失败`);
    }
  }

  // 获取权限位值对应的权限名称列表（用于调试）
  getPermissionNames(permissionValue) {
    const permissionNames = [];
    
    if (permissionValue & PERMISSIONS.MANAGE_USERS) {
      permissionNames.push('user:manage');
    }
    if (permissionValue & PERMISSIONS.MANAGE_PERMISSIONS) {
      permissionNames.push('permission:manage');
    }
    if (permissionValue & PERMISSIONS.MANAGE_EVENTS) {
      permissionNames.push('event:manage');
    }
    if (permissionValue & PERMISSIONS.CREATE_EVENTS) {
      permissionNames.push('event:create');
    }
    if (permissionValue & PERMISSIONS.READ_OWN_EVENTS) {
      permissionNames.push('event:read:own');
    }
    if (permissionValue & PERMISSIONS.READ_PUBLIC_EVENTS) {
      permissionNames.push('event:read:public');
    }
    
    return permissionNames;
  }
}

module.exports = PermissionService;    
