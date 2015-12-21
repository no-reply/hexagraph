require 'fileutils'

module Hexagraph
  ##
  # An LMDB backed graph database
  class Database
    attr_reader :env
    
    ##
    # Initializes a database at the given path
    # 
    # @param path [String] a path to initialize 
    def initialize(path, mapsize: 10_000_000, create: true)
      FileUtils::mkdir_p path if create
      @env = LMDB.new(path, mapsize: mapsize)

      assign_indexes!(create)
    end

    ##
    # @return [Enumerator] an enumerator over the edges in the graph
    def edges
      Enumerator.new do |yielder|
        @spo.cursor do |c|
          loop do
            edge = c.next
            break if edge.nil?
            yielder << edge.first.split("\00") 
          end
        end
      end
    end

    ##
    # @return [void]
    def clear!
      @env.transaction do
        clear_indexes!
      end
    end

    ##
    # @return [Integer] a current count of the number of edges in the 
    #   graph
    def count
      @spo.count
    end

    ##
    # @param n1 [String]
    # @param e [String]
    # @param n2 [String]
    #
    # @return [Boolean] true if the edge was inserted.
    def insert(n1, e, n2)
      @env.transaction { _insert(n1, e, n2) }
    end

    ##
    # @param edges [Enumerable]
    #
    # @return [Boolean] true if the edges were inserted.
    def inserts(edges)
      @env.transaction do
        edges.each { |n1, e, n2| _insert(n1, e, n2) }
      end

      true
    end

    ##
    # @param n1 [String]
    # @param e [String]
    # @param n2 [String]
    #
    # @return [Boolean] true if the edge was deleted; false if it was not 
    #   present
    def delete(n1, e, n2)
      @env.transaction { _delete(n1, e, n2) }
    end

    ##
    # @param edges [Enumerable]
    #
    # @return [Boolean] true if the edges were inserted.
    def deletes(edges)
      @env.transaction do
        edges.each { |n1, e, n2| _delete(n1, e, n2) }
      end
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

    ##
    # @param n1 [String]
    # @param e [String]
    # @param n2 [String]
    # 
    # @return [Boolean] true if a (directed) edge exists between `s` and `o`,
    #   connected by `p`
    def has_edge?(n1, e, n2)
      @spo.cursor do |cursor|
        key = spo_key(n1, e, n2)
        begin
          return true if 
            cursor.set_range(key).first == key
        rescue LMDB::Error::NOTFOUND; end
      end
      
      false
    end

    ##
    # @param n1 [String]
    # @param n2 [String]
    # 
    # @return [Boolean] true if the two nodes are connected by any edge
    def adjacent?(n1, n2)
      @sop.cursor do |cursor|
        begin
          return true if 
            cursor.set_range("#{n1}\00#{n2}\00")
            .first.start_with?("#{n1}\00#{n2}\00")
        rescue LMDB::Error::NOTFOUND; end
      end

      # @todo this sometimes gives a false negative unless using a new cursor, 
      #   why? `cursor.first` does not prevent the failure.
      @sop.cursor do |cursor|
        begin
          return true if 
            cursor.set_range("#{n2}\00#{n1}\00")
            .first.start_with?("#{n2}\00#{n1}\00")
        rescue LMDB::Error::NOTFOUND; end
      end
      false
    end

    
    private

    def assign_indexes!(create)
      @spo = @env.database('spo', create: create)
      @ops = @env.database('ops', create: create)
      @sop = @env.database('sop', create: create)
      @pso = @env.database('pso', create: create)
    end

    def clear_indexes!
      @spo.clear
      @ops.clear
      @sop.clear
      @pso.clear
    end

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
