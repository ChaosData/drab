# frozen_string_literal: false
module DRab
  class DRabObject # :nodoc:
    def ==(other)
      return false unless DRabObject === other
     (@ref == other.__drabref) && (@uri == other.__draburi)
    end

    def hash
      [@uri, @ref].hash
    end

    alias eql? ==
  end
end
