# frozen_string_literal: false
require 'observer'

module DRab
  # The Observable module extended to DRab.  See Observable for details.
  module DRabObservable
    include Observable

    # Notifies observers of a change in state.  See also
    # Observable#notify_observers
    def notify_observers(*arg)
      if defined? @observer_state and @observer_state
        if defined? @observer_peers
          @observer_peers.each do |observer, method|
            begin
              observer.send(method, *arg)
            rescue
              delete_observer(observer)
            end
          end
        end
        @observer_state = false
      end
    end
  end
end
