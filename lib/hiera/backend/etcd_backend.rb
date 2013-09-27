# Hiera backend for the etcd distributed configuration service
class Hiera
  module Backend
    class Etcd_backend

      def initialize
        require 'net/http'
        require 'net/https'
        require 'json'
        @config = Config[:http]
        @http = Net::HTTP.new(@config[:host], @config[:port])
        @http.read_timeout = @config[:http_read_timeout] || 10
        @http.open_timeout = @config[:http_connect_timeout] || 10
      end

      def perform_request(url)
        httpres = nil

        httpreq = Net::HTTP::Get.new("#{url}")
        begin
          httpres = @http.request(httpreq)
        rescue Exception => e
          Hiera.warn("[hiera-etcd]: Net::HTTP threw exception #{e.message}")
          raise Exception, e.message
          return httpres
        end
        unless httpres.kind_of?(Net::HTTPSuccess)
          Hiera.debug("[hiera-etcd]: #{httpres.code} HTTP response for http://#{@config[:host]}:#{@config[:port]}#{url}")
          return nil
        end
        httpres
      end



      def parse_result(result, type, scope)
        res = JSON.parse(result)['value']
        answer = nil
        case type
        when :array
          answer ||= []
          begin
            data = Backend.parse_answer(JSON[res], scope)
            answer << data
          rescue
            Hiera.warn("[hiera-etcd]: '#{res}' is not in json format, and array lookup is requested")
          end
        when :hash
          answer ||= {}
          begin
            data = Backend.parse_answer(JSON[res], scope)
            answer << data
          rescue
            Hiera.warn("[hiera-etcd]: '#{res}' is not in json format, and hash lookup is requested")
          end
        else
          answer = Backend.parse_answer(res, scope)
        end
        answer
      end



      def lookup(key, scope, order_override, resolution_type)
        # Extract multiple etcd paths from the configuration file
        paths = @config[:paths].map { |p| Backend.parse_string(p, scope, { 'key' => key }) }
        paths.insert(0, order_override) if order_override
	answer = nil
        paths.each do |path|
          url = "/v1/keys#{path}/#{key}"
          Hiera.debug("[hiera-etcd]: Lookup http://#{@config[:host]}:#{@config[:port]}#{url}")
          httpres = self.perform_request(url)

          # On to the next path if we don't have a response
          next unless httpres.body

          # Parse result from standard etcd JSON response
          answer = self.parse_result(httpres.body, scope, resolution_type)
          break if answer
        end
        answer
      end

    end
  end
end
