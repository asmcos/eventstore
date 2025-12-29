const {    PERMISSIONS,
    defaultPermissionConfigs
} = require("eventstore-tools/src/common");


// config.js
module.exports = {
  uploaddir:"uploads", 
 // 数据库连接配置
  database: {
    uri: 'mongodb://localhost:27017',
    dbName: 'eventstore',
    // 连接池配置
    collections:{
      events:'events',
      users:'users',
      permissions: 'permissions', // 权限
      comments: 'comments',
      likes: 'likes'
    },
    options: {
      maxPoolSize: 10,
      serverSelectionTimeoutMS: 5000,
    }
  },
  admin:{
    pubkey:"2bc571b20f7dc9734aeca6a1b0c6e5990465f7e19422ecfa8b8cb38f0cec26c1",
    email:"admin@es.com"
  },//pubkey
  defaultPermission:defaultPermissionConfigs.user,

};
