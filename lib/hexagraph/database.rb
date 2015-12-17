require 'fileutils'

module Hexagraph
  ##
  # An LMDB backed graph database
  class Database
    ##
    # Initializes a database at the given path
    # 
    # @param path [String] a path to initialize 
    def initialize(path, create: true)
      FileUtils::mkdir_p path if create
      @lmdb_env = LMDB.new(path, mapsize: 10_000_000)
      @spo = @lmdb_env.database('spo', create: create)
      @ops = @lmdb_env.database('ops', create: create)
      @sop = @lmdb_env.database('sop', create: create)
      @pso = @lmdb_env.database('pso', create: create)
    end

    ##
    #
    # @return [void]
    def clear
      @lmdb_env.transaction do
        @spo.clear
        @ops.clear
        @sop.clear
        @pso.clear
      end
    end

    ##
    # @return [Integer] a current count of the number of edges in the 
    #   graph
    def count
      @spo.count
    end

    ##
    # @return [Boolean] true if the edge was inserted.
    def insert(s, p, o)
      @lmdb_env.transaction { _insert(s, p, o) }
    end

    ##
    # @return [Boolean] true if the edge was deleted; false if it was not 
    #   present
    def delete(s, p, o)
      @lmdb_env.transaction { _delete(s, p, o) }
    end

    ##
    # @return [Boolean] true if there is a node
    def has_node?(node)
      @spo.cursor do |cursor|
        begin
          return true if 
            cursor.set_range(node + "\00").first.start_with?(node + "\00")
        rescue LMDB::Error::NOTFOUND; end
      end

      @ops.cursor do |cursor|
        begin
          return true if 
            cursor.set_range(node + "\00").first.start_with?(node + "\00")
        rescue LMDB::Error::NOTFOUND; end
      end

      false
    end

    private

    def _insert(s, p, o)
      unless @spo[spo_key(s, p, o)]
        @spo[spo_key(s, p, o)] = ''
        @ops[ops_key(s, p, o)] = ''
        @sop[sop_key(s, p, o)] = ''
        @pso[pso_key(s, p, o)] = ''
      end
    end

    def _delete(s, p, o)
      begin
        @spo.delete(spo_key(s, p, o))
      rescue LMDB::Error::NOTFOUND
        return false
      end
      @ops.delete(ops_key(s, p, o))
      @sop.delete(sop_key(s, p, o))
      @pso.delete(pso_key(s, p, o))
    end
    
    def spo_key(s, p, o)
      "#{s}\00#{p}\00#{o}"
    end

    def ops_key(s, p, o)
      "#{o}\00#{p}\00#{s}"
    end

    def sop_key(s, p, o)
      "#{s}\00#{o}\00#{p}"
    end

    def pso_key(s, p, o)
      "#{p}\00#{s}\00#{o}"
    end
  end
end
