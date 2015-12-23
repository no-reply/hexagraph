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
      @dict = Dictionary.new(@env)

      assign_indexes!(create)
    end

    ##
    # @return [Enumerator] an enumerator over the edges in the graph
    def edges(graph: DEFAULT_GRAPH)
      graph = @dict.get(graph)

      Enumerator.new do |yielder|
        @gspo.cursor do |c|
          begin
            c.set_range("#{graph}#{separator}")
          rescue LMDB::Error::NOTFOUND; break; end
          
          edge = c.get

          loop do
            edge = split_key(edge.first)
            break if edge.first != graph

            yielder << edge[1..3].map { |t| @dict.lookup(t) }
            
            edge = c.next
            break if edge.nil? 
          end
        end
      end
    end

    ##
    # @return [void]
    def clear!
      @env.transaction do
        clear_indexes!
        @dict.clear!
      end
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
      graph = @dict.get(graph)

      @gspo.cursor do |cursor|
        begin
          return true if 
            cursor.set_range("#{graph}#{separator}")
            .first.start_with?("#{graph}#{separator}")
        rescue LMDB::Error::NOTFOUND; end
      end

      false
    end

    ##
    # @return [Boolean] true if there is a node
    def has_node?(node, graph: DEFAULT_GRAPH)
      node  = @dict.get(node)
      graph = @dict.get(graph)

      @gspo.cursor do |cursor|
        begin
          return true if 
            cursor.set_range([graph, node].join(separator))
            .first.start_with?([graph, node].join(separator))
        rescue LMDB::Error::NOTFOUND; end
      end

      @gosp.cursor do |cursor|
        begin
          return true if
            cursor.set_range([graph, node].join(separator))
            .first.start_with?([graph, node].join(separator))
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
        key = gspo_key(@dict.get(n1), 
                       @dict.get(e), 
                       @dict.get(n2), 
                       @dict.get(graph))
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
      n1    = @dict.get(n1)
      n2    = @dict.get(n2)
      graph = @dict.get(graph)

      @gosp.cursor do |cursor|
        begin
          return true if 
            cursor.set_range([graph, n1, n2].join(separator))
            .first.start_with?([graph, n1, n2].join(separator))
        rescue LMDB::Error::NOTFOUND; end
      end

      # @todo this sometimes gives a false negative unless using a new cursor, 
      #   why? `cursor.first` does not prevent the failure.
      @gosp.cursor do |cursor|
        begin
          return true if 
            cursor.set_range([graph, n2, n1].join(separator))
            .first.start_with?([graph, n2, n1].join(separator))
        rescue LMDB::Error::NOTFOUND; end
      end

      false
    end
    
    private

    def assign_indexes!(create)
      @spog = @env.database('spog', create: create)
      @ospg = @env.database('ospg', create: create)
      @psog = @env.database('psog', create: create)
      @posg = @env.database('posg', create: create)
      @gspo = @env.database('gspo', create: create)
      @gosp = @env.database('gosp', create: create)
      @gpso = @env.database('gpso', create: create)
      @gpos = @env.database('gpos', create: create)
    end

    def clear_indexes!
      @spog.clear
      @ospg.clear
      @psog.clear
      @posg.clear
      @gspo.clear
      @gosp.clear
      @gpso.clear
      @gpos.clear
    end

    def _insert(s, p, o, g)
      s = @dict.get(s)
      p = @dict.get(p)
      o = @dict.get(o)
      g = @dict.get(g)

      unless @spog[spog_key(s, p, o, g)]
        @spog[spog_key(s, p, o, g)] = ''
        @ospg[ospg_key(s, p, o, g)] = ''
        @psog[psog_key(s, p, o, g)] = ''
        @posg[posg_key(s, p, o, g)] = ''
        @gspo[gspo_key(s, p, o, g)] = ''
        @gosp[gosp_key(s, p, o, g)] = ''
        @gpso[gpso_key(s, p, o, g)] = ''
        @gpos[gpos_key(s, p, o, g)] = ''
      end
    end

    def _delete(s, p, o, g)
      s = @dict.get(s)
      p = @dict.get(p)
      o = @dict.get(o)
      g = @dict.get(g)

      begin
        @spog.delete(spog_key(s, p, o, g))
      rescue LMDB::Error::NOTFOUND
        return false
      end
      @ospg.delete ospg_key(s, p, o, g)
      @psog.delete psog_key(s, p, o, g)
      @posg.delete posg_key(s, p, o, g)
      @gspo.delete gspo_key(s, p, o, g)
      @gosp.delete gosp_key(s, p, o, g)
      @gpso.delete gpso_key(s, p, o, g)
      @gpos.delete gpos_key(s, p, o, g)
    end

    def separator
      Dictionary::SEPARATOR
    end

    def split_key(key)
      result = [a=''.force_encoding('ASCII-8BIT')]

      key.chars.each do |c|
        if c == separator.force_encoding('ASCII-8BIT')
          result << a=''.force_encoding('ASCII-8BIT')
        else
          a << c
        end
      end

      result.pop if a.empty?

      result
    end

    ##
    # @todo: refactor keys
    def spog_key(s, p, o, g)
      "#{s}#{separator}#{p}#{separator}#{o}#{separator}#{g}"
    end

    def ospg_key(s, p, o, g)
      "#{o}#{separator}#{s}#{separator}#{p}#{separator}#{g}"
    end

    def psog_key(s, p, o, g)
      "#{p}#{separator}#{s}#{separator}#{o}#{separator}#{g}"
    end

    def posg_key(s, p, o, g)
      "#{p}#{separator}#{o}#{separator}#{s}#{separator}#{g}"
    end

    def gspo_key(s, p, o, g)
      "#{g}#{separator}#{s}#{separator}#{p}#{separator}#{o}"
    end

    def gosp_key(s, p, o, g)
      "#{g}#{separator}#{o}#{separator}#{s}#{separator}#{p}"
    end

    def gpso_key(s, p, o, g)
      "#{g}#{separator}#{p}#{separator}#{s}#{separator}#{o}"
    end

    def gpos_key(s, p, o, g)
      "#{g}#{separator}#{p}#{separator}#{o}#{separator}#{s}"
    end
  end
end
