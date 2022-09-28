require 'fluent/plugin/filter'

module Fluent::Plugin
  class SplitArrayFilter < Filter
    Fluent::Plugin.register_filter('split_array', self)

    desc "Key name to split"
    config_param :split_key, :string, default: nil

    def filter_stream(tag, es)
      new_es = Fluent::MultiEventStream.new
      es.each {|time, record|
        target_record = @split_key.nil? ? record : record[@split_key] || {}
        distributed_record = {}
        if !@split_key.nil?
          record.each_pair do |k, v|
            if k == @split_key
              next
            end
            distributed_record[k] = v
          end
        end 
        split(time, target_record, new_es, distributed_record)
      }
      new_es
    end

    private

    def split(time, record, new_es, distributed_record)
      if record.instance_of?(Array)
        distributed_record.each_pair do |k, v|
          record.each { |r| r[k] = v}
        end
        record.each { |r| new_es.add(time, r) }
      else
        distributed_record.each_pair do |k, v|
          record[k] = v
        end
        new_es.add(time, record)
      end
    end
  end
end