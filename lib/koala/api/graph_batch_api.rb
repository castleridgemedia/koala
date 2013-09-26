require 'koala/api'
require 'koala/api/batch_operation'

module Koala
  module Facebook
    # @private
    class GraphBatchAPI < API

      COUNT_LIMIT = 575
      COUNT_PERIOD = 10.minutes

      def get_count_key(access_token)
        "request_count:facebook:#{access_token}:count"
      end

      def get_start_key(access_token)
        "request_count:facebook:#{access_token}:start"
      end

      def redis_incr
        redis = Redis.new
        count_key = get_count_key(@access_token)
        start_key = get_start_key(@access_token)

        req_count = redis.get(count_key)
        if !req_count
          req_count = 0
          redis.set(count_key, req_count)
        else
          req_count = req_count.to_i
        end

        req_start = redis.get(start_key)
        if !req_start
          req_start = Time.now.utc.to_i
          redis.set(start_key, req_start)
        else
          req_start = req_start.to_i
        end
        redis.incr(count_key)

        if (Time.now.utc.to_i - req_start) > COUNT_PERIOD
          redis.set(count_key, 1)
          redis.set(start_key, Time.now.utc.to_i)
        else
          if req_count >= COUNT_LIMIT
            log "sleeping"
            sleep 60
          end
        end
        [redis, count_key, start_key]
      end

      # inside a batch call we can do anything a regular Graph API can do
      include GraphAPIMethods
      MAX_BATCH_SET_SIZE = 10

      attr_reader :original_api
      def initialize(access_token, api)
        super(access_token)
        @original_api = api
      end

      def batch_calls
        @batch_calls ||= []
      end

      def set_batch_calls (b)
        @batch_calls = b
      end

      def graph_call_in_batch(path, args = {}, verb = "get", options = {}, &post_processing)
        # for batch APIs, we queue up the call details (incl. post-processing)
        batch_calls << BatchOperation.new(
          :url => path,
          :args => args,
          :method => verb,
          :access_token => options['access_token'] || access_token,
          :http_options => options,
          :post_processing => post_processing
        )
        nil # batch operations return nothing immediately
      end

      # redefine the graph_call method so we can use this API inside the batch block
      # just like any regular Graph API
      alias_method :graph_call_outside_batch, :graph_call
      alias_method :graph_call, :graph_call_in_batch

      # execute the queued batch calls
      def execute(http_options = {})
        return [] unless batch_calls.length > 0
        return execute_partial(http_options) unless batch_calls.length > MAX_BATCH_SET_SIZE

        temp_batch_calls = batch_calls
=begin
        # old
        temp_batch_calls = batch_calls.reverse!
        full_results = []
        while temp_batch_calls.length > 0
          set_batch_calls(temp_batch_calls.pop(MAX_BATCH_SET_SIZE).reverse)
          full_results << execute_partial(http_options)
        end
        set_batch_calls([])
        full_results.flatten
        # /old
=end
        # new
        full_results = batch_calls.collect { nil }
        ((temp_batch_calls.count / MAX_BATCH_SET_SIZE) + 1).times do
          @access_token = access_token
          redis_incr
        end

        max_threads = 10
        slices = []
        temp_batch_calls.each_with_index.each_slice(MAX_BATCH_SET_SIZE) do |slice|
          slices << slice
        end

        slices.each_slice(max_threads) do |slice_slice|
          threads = []
          slice_slice.each do |slice|
            threads << Thread.new {
              graph = Koala::Facebook::API.new(self.access_token)
              indices = []
              calls = []
              slice.each do |request, index|
                calls << request
                indices << index
              end

              inner_results = graph.batch do |b|
                b.set_batch_calls(calls)
              end

              inner_results.zip(indices).each do |result, index|
                full_results[index] = result
              end
            }

          end
          threads.each { |thread| thread.join }
        end
        set_batch_calls([])
        full_results
        # /new
      end

      def execute_partial(http_options = {})
        return [] unless batch_calls.length > 0
        # Turn the call args collected into what facebook expects
        args = {}
        args["batch"] = MultiJson.dump(batch_calls.map { |batch_op|
          args.merge!(batch_op.files) if batch_op.files
          batch_op.to_batch_params(access_token)
        })

        batch_result = graph_call_outside_batch('/', args, 'post', http_options) do |response|
          unless response
            # Facebook sometimes reportedly returns an empty body at times
            # see https://github.com/arsduo/koala/issues/184
            raise BadFacebookResponse.new(200, '', "Facebook returned an empty body")
          end

          # map the results with post-processing included
          index = 0 # keep compat with ruby 1.8 - no with_index for map
          response.map do |call_result|
            # Get the options hash
            batch_op = batch_calls[index]
            index += 1

            raw_result = nil
            if call_result
              if ( error = check_response(call_result['code'], call_result['body'].to_s) )
                raw_result = error
              else
                # (see note in regular api method about JSON parsing)
                body = MultiJson.load("[#{call_result['body'].to_s}]")[0]

                # Get the HTTP component they want
                raw_result = case batch_op.http_options[:http_component]
                when :status
                  call_result["code"].to_i
                when :headers
                  # facebook returns the headers as an array of k/v pairs, but we want a regular hash
                  call_result['headers'].inject({}) { |headers, h| headers[h['name']] = h['value']; headers}
                else
                  body
                end
              end
            end

            # turn any results that are pageable into GraphCollections
            # and pass to post-processing callback if given
            result = GraphCollection.evaluate(raw_result, @original_api)
            if batch_op.post_processing
              batch_op.post_processing.call(result)
            else
              result
            end
          end
        end
      end

    end
  end
end
