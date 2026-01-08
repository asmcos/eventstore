const { getClient } = require('./client');
const config = require('../../config/config');
const { verifyEvent } = require("eventstore-tools/src/key");

class BrowseLogsService {
  constructor() {
    this.collections = config.database.collections;
    this.adminPubkey = config.admin.pubkey;
    // 可配置的重复记录间隔（毫秒），方便后续调整
    this.duplicateInterval = 24 * 60 * 60 * 1000; // 24小时
    // 绑定核心方法上下文
    this.reportBrowseLog = this.reportBrowseLog.bind(this);
    
    this.readBrowseLogs = this.readBrowseLogs.bind(this);
    this.counts = this.countBrowseLogs.bind(this);
  }

  // 获取数据库实例（复用共享客户端，与点赞服务保持一致）
  async getDb() {
    const client = await getClient();
    return client.db(config.database.dbName);
  }

  /**
   * 上报浏览记录（适配code:700，支持匿名用户，24小时去重）
   * @param {Object} browseEvent - 浏览事件（含user/anonymousId/targetType/targetId等）
   * @returns {Object} 操作结果
   */
  async reportBrowseLog(browseEvent) {
    const db = await this.getDb();
    const browsesCollection = db.collection(this.collections.browselogs);

    // 1. 校验专属code码
    if (browseEvent.code !== 700) {
      return { code: 400, message: '无效浏览记录code码，仅支持700' };
    }

    // 2. 时间校验（5分钟容忍度，和点赞服务保持一致）
    const clientTime = new Date(browseEvent.created_at || Date.now());
    const timeDiff = Math.abs(Date.now() - clientTime.getTime());
    if (timeDiff > 5 * 60 * 1000) {
      return { code: 500, message: '时间和服务器差距太大' };
    }

 
    if (!browseEvent.targetId) {
      return { code: 400, message: '目标内容ID targetId不能为空' };
    }

    // 4. 24小时内查重逻辑（核心修改）
    // 计算24小时前的时间戳
    const before24h = new Date(Date.now() - this.duplicateInterval);
    // 构造查重条件：同一用户/匿名ID + 同一targetId + 24小时内的记录
    const duplicateQuery = {
      targetId: browseEvent.targetId,
      createdAt: { $gte: before24h }
    };
    // 区分登录用户和匿名用户的查重逻辑
    if (browseEvent.user) {
      // 登录用户：按user字段查重（优先级更高）
      duplicateQuery.user = browseEvent.user;
    }  

    // 检查是否存在重复记录
    const existingRecord = await browsesCollection.findOne(duplicateQuery);
    if (existingRecord) {
      return {
        code: 200,
        message: `24小时内已记录该内容的浏览，无需重复上报`,
        data: {
          browseId: existingRecord._id,
          lastBrowseTime: existingRecord.createdAt
        }
      };
    }

    // 5. 构造浏览记录数据
    const browseRecord = {
      user: browseEvent.user || null, // 登录用户ID/公钥，匿名则为null
      anonymousId: browseEvent.anonymousId, // 匿名标识（必填）
      targetId: browseEvent.targetId, // 目标内容ID
      ipAddress: browseEvent.ipAddress || '', // IP地址（可选）
      createdAt: new Date(),
      updatedAt: new Date()
    };

    // 6. 写入浏览记录（无重复才插入）
    await browsesCollection.insertOne(browseRecord);

    return {
      code: 200,
      message: '浏览记录上报成功',
      data: {
        browseId: browseRecord._id,
        browseTime: browseRecord.createdAt // 修正原代码的browseTime拼写错误
      }
    };
  }

  /**
   * 查询浏览记录（适配code:703，支持用户/管理员维度）
   * @param {Object} queryEvent - 查询事件（含user/anonymousId/targetType/targetId等）
   * @returns {Object} 浏览记录列表
   */
  async readBrowseLogs(queryEvent) {
    const db = await this.getDb();
    const browsesCollection = db.collection(this.collections.browselogs);

    // 1. 校验专属code码
    if (queryEvent.code !== 703) {
      return { code: 400, message: '无效浏览记录查询code码，仅支持703' };
    }

    // 2. 构造查询条件
    const query = {};

    // 普通用户：仅能查询自己的记录（含匿名期）
    if (queryEvent.user !== this.adminPubkey) {
      query.$or = [
        { user: queryEvent.user }, // 登录后的浏览记录
        { anonymousId: queryEvent.anonymousId, user: null } // 匿名期的浏览记录
      ];
    } else {
      // 管理员：可按条件查询所有记录
      if (queryEvent.userFilter) query.user = queryEvent.userFilter; // 按用户筛选
      if (queryEvent.anonymousId) query.anonymousId = queryEvent.anonymousId; // 按匿名ID筛选
    }

    // 通用筛选条件
    if (queryEvent.targetType) query.targetType = queryEvent.targetType; // 按目标类型筛选
    if (queryEvent.targetId) query.targetId = queryEvent.targetId; // 按目标ID筛选
    if (queryEvent.startTime && queryEvent.endTime) {
      query.createdAt = { // 修正为createdAt（原代码的browseTime不存在）
        $gte: new Date(queryEvent.startTime),
        $lte: new Date(queryEvent.endTime)
      }; // 按时间范围筛选
    }

    // 3. 分页参数（默认分页，避免数据量过大）
    const pageNum = queryEvent.pageNum || 1;
    const pageSize = queryEvent.pageSize || 20;
    const skip = (pageNum - 1) * pageSize;

    // 4. 执行查询
    const browseRecords = await browsesCollection
      .find(query)
      .sort({ createdAt: -1 }) // 修正为createdAt倒序
      .skip(skip)
      .limit(pageSize)
      .toArray();

    // 5. 格式化返回结果（隐藏敏感信息，如IP）
    const formattedRecords = browseRecords.map(record => ({
      _id: record._id,
      user: record.user,
      anonymousId: record.anonymousId,
      targetId: record.targetId,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
      // 管理员才返回IP地址
      ipAddress: queryEvent.user === this.adminPubkey ? record.ipAddress : '******'
    }));

    return {
      code: 200,
      message: '浏览记录查询成功',
      data: {
        list: formattedRecords,
        pageNum,
        pageSize,
        total: await browsesCollection.countDocuments(query)
      }
    };
  }

  /**
   * 统计浏览记录总数/浏览量（适配code:704，支持多维度统计）
   * @param {Object} countEvent - 统计事件（含targetType/targetId/statType等）
   * @returns {Object} 统计结果
   */
  async countBrowseLogs(countEvent) {
    const db = await this.getDb();
    const browsesCollection = db.collection(this.collections.browselogs);

    // 1. 校验专属code码
    if (countEvent.code !== 704) {
      return { code: 400, message: '无效浏览记录统计code码，仅支持704' };
    }

    // 2. 构造筛选条件
    const filter = {};
    
    if (Array.isArray(countEvent.targetId)) {
        const matchStage = {
          targetId: { $in: countEvent.targetId }
        };
    
        const aggregationPipeline = [
          { $match: matchStage },
          {
            $group: {
              _id: "$targetId",
              count: { $sum: 1 }
            }
          },
          {
            $project: {
              targetId: "$_id",
              count: 1,
              _id: 0
            }
          }
        ];
    
        const result = await browsesCollection.aggregate(aggregationPipeline).toArray();
        return {code:200,message:'统计成功',counts:result}
    } else {

        filter.targetId = countEvent.targetId;
              // 3. 执行统计
        const total = await browsesCollection.countDocuments(filter);
        return {
          code: 200,
          message: '统计成功',
          counts: total
        };
      
    }
  

  }
}

module.exports = BrowseLogsService;
