# frozen_string_literal: true

module Rhino
  require 'redis'
  require 'json'
  require 'observer'
  require 'etc'

  class Queue
    include Observable

    def initialize(name:)
      @name = name
      @redis = Redis.new(host: '127.0.0.1', port: 6379, db: 6)
    end

    def add(job_name:, data:)
      changed
      key = "rhino:#{@name}:#{job_name}"
      @redis.set(key, data.to_json)
      notify_observers(key)
    end
  end

  class Worker
    def initialize(queue:, handler:)
      @queue = queue
      @handler = handler
      @redis = Redis.new(host: '127.0.0.1', port: 6379, db: 6)
      max_cores = Etc.nprocessors
      @queue.add_observer(self, :process_queue)

      @supervisor = Ractor.new [max_cores, @redis, @handler] do |max_cores, redis, handler|
        actors = []
        max_cores.times do |core|
          actors << Ractor.new(core) do |core|
            config = Ractor.receive
            data_obj = JSON.parse(config[:redis].get(config[:key]))
            method(config[:handler]).call(config[:key].split(':').last, data_obj)
            config[:redis].del(config[:key])
          end
        end

        # keep alive
        loop do
          job_key = Ractor.receive

          # re-pop actors
          if actors.size == 0
            max_cores.times do |core|
              actors << Ractor.new(core) do |core|
                config = Ractor.receive
                data_obj = JSON.parse(config[:redis].get(config[:key]))
                method(config[:handler]).call(config[:key].split(':').last, data_obj)
                config[:redis].del(config[:key])
              end
            end
          end

          actor = actors.pop()
          actor.send({ key: job_key, redis: redis, handler: handler })
        end
      end
    end

    def process_queue(key)
      @supervisor.send(key)
    end

  end
end

r = Rhino::Queue.new(name: 'tester')

def handler(job_name, data)
  sleep data['sleep_time'].to_i
  p "completed job #{job_name}"
end

Rhino::Worker.new(queue: r, handler: :handler)

r.add(job_name: 'first_job', data: { sleep_time: 10 })

sleep 2

r.add(job_name: 'second_extra', data: { sleep_time: 5 })

10.times do |i|
  sleep 1
  r.add(job_name: "job_#{i}", data: { sleep_time: 2 })
end

loop do
  # keep locally running until daamon
end
