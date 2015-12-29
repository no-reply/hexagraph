require 'rdf'

module Hexagraph
  class Repository < RDF::Repository
    DEFAULT_GRAPH = false

    def initialize(*args, &block)
      @db = Database.new('.tmp/rdf')
      super
    end

    ##
    # @private
    # @see RDF::Enumerable#supports?
    def supports?(feature)
      case feature.to_sym
        #statement named graphs
        when :graph_name   then @options[:with_graph_name]
        when :inference then false  # forward-chaining inference
        when :validity  then @options.fetch(:with_validity, true)
        else false
      end
    end

    ##
    # @see RDF::Enumerable#count
    def count
      @db.count
    end

    ##
    # @see RDF::Durable#durable?
    def durable?
      true
    end

    ##
    # @private
    # @see RDF::Enumerable#each_statement
    def each_statement(&block)
      if block_given?
        @db.edges.each do |s, p, o|
          yield RDF::Statement.new(to_term(s),
                                   to_term(p),
                                   to_term(o))
        end
      else
        enum_statement
      end
    end
    alias_method :each, :each_statement

    ##
    # @private
    # @see RDF::Enumerable#has_graph?
    def has_graph?(value)
      @db.has_graph?(value)
    end

    ##
    # @private
    # @see RDF::Enumerable#has_statement?
    def has_statement?(statement)
      @db.has_edge?(*statement.to_a.map(&:to_base))
    end

    ##
    # @private
    # @see RDF::Enumerable#each_graph
    def each_graph(&block)
      []
    end

    protected

     ##
     # @private
     # @see RDF::Mutable#clear
     def clear_statements
       @db.clear!
     end

     ##
     # @private
     # @see RDF::Mutable#delete
     def delete_statement(statement)
       @db.delete(statement.subject.to_base,
                  statement.predicate.to_base,
                  statement.object.to_base,
                  graph: statement.graph_name)
       self
     end

     ##
     # @private
     # @see RDF::Mutable#insert
     def delete_statements(statements)
       require 'pry'; binding.pry
       self
     end

     ##
     # @private
     # @see RDF::Mutable#insert
     def insert_statement(statement)
       @db.insert(statement.subject.to_base,
                  statement.predicate.to_base,
                  statement.object.to_base,
                  graph: statement.graph_name)
       self
     end

     ##
     # @private
     # @see RDF::Mutable#insert
     # def insert_statements(statements)
     #   require 'pry'; binding.pry
     #   self
     # end

     ##
     # Match elements with eql?, not ==
     # Context of `false` matches default graph. Unbound variable matches non-false graph name
     # @private
     # @see RDF::Queryable#query
     def query_pattern(pattern, options = {}, &block)
       require 'pry'; binding.pry
     end

    private

     ##
     # Converts an NTriples (RDF::Term#to_base) string to an RDF::Term
     #
     # @param str [String]
     # @return [RDF::Term]
     def to_term(str)
       RDF::NTriples::Reader.unserialize(str)
     end
  end
end
