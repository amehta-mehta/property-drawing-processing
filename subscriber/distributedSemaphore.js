const { Semaphore } = require('async-mutex');

class LocalSemaphore {
  constructor(name, limit) {
    this.name = name;
    this.limit = limit;
    this.semaphore = new Semaphore(limit);
    
    console.log(`✅ Local semaphore initialized for ${name} with limit: ${limit}`);
  }

  async acquire() {
    const [, release] = await this.semaphore.acquire();
    return release;
  }

  async getAvailablePermits() {
    return this.semaphore.getValue();
  }

  async getCurrentUsage() {
    return this.limit - this.semaphore.getValue();
  }

  isRedisConnected() {
    return false; // No Redis in local-only mode
  }

  getRedisConfig() {
    return null; // No Redis configuration
  }
}

module.exports = { LocalSemaphore };