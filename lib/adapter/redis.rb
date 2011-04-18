require 'adapter'
require 'redis'

module Adapter
  module Redis
    def read(key)
      if @options[:hash]
        decode(client.hget(@options[:hash], key_for(key)))
      else
        decode(client.get(key_for(key)))
      end
    end

    def write(key, value)
      if @options[:hash]
        client.hset(@options[:hash],key_for(key), encode(value))
      else
        client.set(key_for(key), encode(value))    
      end
    end

    def delete(key)
      read(key).tap do
        if @options[:hash]
          client.hdel(@options[:hash],key_for(key))
        else
          client.del(key_for(key))
        end
      end
    end
    
    def count
      if @options[:hash]
        client.hlen(@options[:hash])
      else
        raise StandardError, "option :hash required for counting"
      end
    end

    def all_keys
      if @options[:hash]
        client.hkeys(@options[:hash])
      else
        raise StandardError, "option :hash required for retrieving all keys"
      end
    end
    
    def clear
      client.flushdb
    end

    # Pretty much stolen from redis objects
    # http://github.com/nateware/redis-objects/blob/master/lib/redis/lock.rb
    def lock(name, options={}, &block)
      key           = name.to_s
      start         = Time.now
      acquired_lock = false
      expiration    = nil
      expires_in    = options.fetch(:expiration, 1)
      timeout       = options.fetch(:timeout, 5)

      while (Time.now - start) < timeout
        expiration    = generate_expiration(expires_in)
        acquired_lock = client.setnx(key, expiration)
        break if acquired_lock

        old_expiration = client.get(key).to_f

        if old_expiration < Time.now.to_f
          expiration     = generate_expiration(expires_in)
          old_expiration = client.getset(key, expiration).to_f

          if old_expiration < Time.now.to_f
            acquired_lock = true
            break
          end
        end

        sleep 0.1
      end

      raise(LockTimeout.new(name, timeout)) unless acquired_lock

      begin
        yield
      ensure
        client.del(key) if expiration > Time.now.to_f
      end
    end

    # Defaults expiration to 1
    def generate_expiration(expiration)
      (Time.now + (expiration || 1).to_f).to_f
    end
  end
end

Adapter.define(:redis, Adapter::Redis)