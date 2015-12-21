require 'fileutils'

module Hexagraph
  ##
  # An LMDB backed graph database
  class Database
    attr_reader :env
    
    DEFAULT_GRAPH = '_g'
    
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
    def edges(graph: DEFAULT_GRAPH)
      Enumerator.new do |yielder|
        @gspo.cursor do |c|
          begin
            c.set_range("#{graph}\00")
          rescue LMDB::Error::NOTFOUND; break; end
          
          edge = c.get

          loop do
            edge = edge.first.split("\00")
            break if edge.first != graph
            yielder << edge[1..3]

            edge = c.next
            break if edge.nil? 
          end
        end
      end
    end

    ##
    # @return [void]
    def clear!
      @env.transaction { clear_indexes! }
    end

    ##
    # @return [Integer] a current count of the number of edges in the 
    #   graph
    def count
      @spog.count
    end

    ##
    # @param n1 [String]
    # @param e [String]
    # @param n2 [String]
    #
    # @return [Boolean] true if the edge was inserted.
    def insert(n1, e, n2, graph: DEFAULT_GRAPH)
      @env.transaction { _insert(n1, e, n2, graph) }
    end

    ##
    # @param edges [Enumerable]
    #
    # @return [Boolean] true if the edges were inserted.
    def inserts(edges, graph: DEFAULT_GRAPH)
      @env.transaction do
        edges.each do |n1, e, n2, g|
          g ||= graph
          _insert(n1, e, n2, g)
        end
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
    def delete(n1, e, n2, graph: DEFAULT_GRAPH)
      @env.transaction { _delete(n1, e, n2, graph) }
    end

    ##
    # @param edges [Enumerable]
    #
    # @return [Boolean] true if the edges were inserted.
    def deletes(edges, graph: DEFAULT_GRAPH)
      @env.transaction do
        edges.each do |n1, e, n2, g| 
          g ||= graph
          _delete(n1, e, n2, g)
        end
      end
    end

    ##
    # @return [Boolean] true if graph exists
    def has_graph?(graph)
      @gspo.cursor do |cursor|
        begin
          return true if 
            cursor.set_range("#{graph}\00")
            .first.start_with?("#{graph}\00")
        rescue LMDB::Error::NOTFOUND; end
      end

      false
    end

    ##
    # @return [Boolean] true if there is a node
    def has_node?(node, graph: DEFAULT_GRAPH)
      @gspo.cursor do |cursor|
        begin
          return true if 
            cursor.set_range("#{graph}\00#{node}\00")
            .first.start_with?("#{graph}\00#{node}\00")
        rescue LMDB::Error::NOTFOUND; end
      end

      @gops.cursor do |cursor|
        begin
          return true if
            cursor.set_range("#{graph}\00#{node}\00")
            .first.start_with?("#{graph}\00#{node}\00")
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
    def has_edge?(n1, e, n2, graph: DEFAULT_GRAPH)
      @gspo.cursor do |cursor|
        key = gspo_key(n1, e, n2, graph)
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
    def adjacent?(n1, n2, graph: DEFAULT_GRAPH)
      @gsop.cursor do |cursor|
        begin
          return true if 
            cursor.set_range("#{graph}\00#{n1}\00#{n2}\00")
            .first.start_with?("#{graph}\00#{n1}\00#{n2}\00")
        rescue LMDB::Error::NOTFOUND; end
      end

      # @todo this sometimes gives a false negative unless using a new cursor, 
      #   why? `cursor.first` does not prevent the failure.
      @gsop.cursor do |cursor|
        begin
          return true if 
            cursor.set_range("#{graph}\00#{n2}\00#{n1}\00")
            .first.start_with?("#{graph}\00#{n2}\00#{n1}\00")
        rescue LMDB::Error::NOTFOUND; end
      end

      false
    end
    
    private

    def assign_indexes!(create)
      @spog = @env.database('spog', create: create)
      @psog = @env.database('psog', create: create)
      @gspo = @env.database('gspo', create: create)
      @gops = @env.database('gops', create: create)
      @gsop = @env.database('gsop', create: create)
    end

    def clear_indexes!
      @spog.clear
      @psog.clear
      @gspo.clear
      @gops.clear
      @gsop.clear
    end

    def _insert(s, p, o, g)
      unless @spog[spog_key(s, p, o, g)]
        @spog[spog_key(s, p, o, g)] = ''
        @psog[psog_key(s, p, o, g)] = ''
        @gspo[gspo_key(s, p, o, g)] = ''
        @gops[gops_key(s, p, o, g)] = ''
        @gsop[gsop_key(s, p, o, g)] = ''
      end
    end

    def _delete(s, p, o, g)
      begin
        @spog.delete(spog_key(s, p, o, g))
      rescue LMDB::Error::NOTFOUND
        return false
      end
      @psog.delete(psog_key(s, p, o, g))
      @gspo.delete(gspo_key(s, p, o, g))
      @gops.delete(gops_key(s, p, o, g))
      @gsop.delete(gsop_key(s, p, o, g))
    end
    
    def spog_key(s, p, o, g)
      "#{s}\00#{p}\00#{o}\00#{g}"
    end

    def psog_key(s, p, o, g)
      "#{p}\00#{s}\00#{o}\00#{g}"
    end

    def gspo_key(s, p, o, g)
      "#{g}\00#{s}\00#{p}\00#{o}"
    end

    def gops_key(s, p, o, g)
      "#{g}\00#{o}\00#{p}\00#{s}"
    end

    def gsop_key(s, p, o, g)
      "#{g}\00#{s}\00#{o}\00#{p}"
    end
  end
end
