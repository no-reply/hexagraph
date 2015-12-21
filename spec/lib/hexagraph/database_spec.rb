require 'spec_helper'
require 'fileutils'
require 'pry'

describe Hexagraph::Database do
  subject { described_class.new('.tmp/spec_db') }

  after(:context) do
    FileUtils.rm Dir.glob('.tmp/spec_*/*.mdb')
    FileUtils.rmdir Dir.glob('.tmp/spec_*')
  end

  after { subject.clear! }

  def random_nodes(n)
    (0..(n - 1)).each_with_object([]) do |_, arry|
      arry << (0...10).map { (65 + rand(26)).chr }.join
    end
  end

  def random_edges(n)
    random_nodes(n * 3).each_slice(3).to_a
  end

  shared_context 'with edges' do
    before { edges.each { |s| subject.insert(*s) } }

    let(:edges) { random_edges(5) }
  end

  xdescribe 'benchmark' do
    require 'benchmark'
    
    it 'bm' do
      FileUtils::mkdir_p '.tmp/bm' 
      lmdb_env = LMDB.new('.tmp/bm', mapsize: 10_000_000_000)

      db = lmdb_env.database

      5.times do 
        lmdb_env.transaction do
          puts '.'
          1_000_000.times do
            s = (0...10).map { (65 + rand(26)).chr }.join
            p = (0...10).map { (65 + rand(26)).chr }.join
            o = (0...10).map { (65 + rand(26)).chr }.join
            c = (0...10).map { (65 + rand(26)).chr }.join
            
            db["#{s}\00#{p}\00#{o}\00#{c}"] = ''
          end
        end
      end

      puts Benchmark.measure {
        lmdb_env.transaction do
          50_000.times do
            s = (0...12).map { (65 + rand(26)).chr }.join
            p = (0...12).map { (65 + rand(26)).chr }.join
            o = (0...12).map { (65 + rand(26)).chr }.join
            c = (0...12).map { (65 + rand(26)).chr }.join
            
            db["#{s}\00#{p}\00#{o}\00#{c}"] = ''
          end
        end
      }

      puts Benchmark.measure {
        1000.times do
          s = (0...10).map { (65 + rand(26)).chr }.join
          subject.has_node?(s)
        end
      }
    end
  end

  describe 'initializing' do
    it 'sets mapsize' do
      size = 100_000
      small = described_class.new('.tmp/spec_sm', mapsize: 100_000)

      expect(small.env.info[:mapsize]).to eq size
    end

    context 'when create is false' do
      it 'succeeds on existing path' do
        expect { subject }.not_to raise_error
      end

      it 'fails creating db at new path' do
        expect { described_class.new('.tmp/spec_fail', create: false) }
          .to raise_error LMDB::Error
      end
    end
  end

  describe '#edges' do
    it 'is empty' do
      expect(subject.edges.count).to eq 0
    end
    
    context 'with edges' do
      include_context 'with edges'

      it 'is an Enumerable' do
        expect(subject.edges).to be_a Enumerable
      end
      
      it 'enumerates edges' do
        expect(subject.edges).to contain_exactly(*edges)
      end
    end
  end

  describe '#insert' do
    it 'inserts an edge' do
      expect { subject.insert('a', 'b', 'c') }
        .to change { subject.count }.from(0).to(1)
    end

    it 'has the edge after insert' do
      expect { subject.insert('a', 'b', 'c') }
        .to change { subject.has_edge?('a', 'b', 'c') }.to(true)
    end

    it 'does not re-insert' do
      subject.insert('a', 'b', 'c')
      expect { subject.insert('a', 'b', 'c') }
        .not_to change { subject.count }
    end

    context 'to graph' do
      let(:edge) { random_edges(1).first }

      it 'inserts to correct graph' do
        expect { subject.insert(*edge, graph: 'g') }
          .to change { subject.edges(graph: 'g') }.to contain_exactly(edge)
      end

      it 'does not insert to default graph' do
        expect { subject.insert(*edge, graph: 'g') }
          .not_to change { subject.edges.to_a }
      end
    end
  end

  describe '#delete' do
    before { subject.insert('a', 'b', 'c') }

    it 'deletes the edge' do
      expect { subject.delete('a', 'b', 'c') }
        .to change { subject.count }.from(1).to(0)
    end
    
    it 'does not delete non-existent edge' do
      expect { subject.delete('a', 'b', 'd') }.not_to change { subject.count }
    end
  end

  describe '#has_node?' do
    before { subject.insert('a', 'b', 'c') }

    it 'has subject nodes' do
      expect(subject).to have_node 'a'
    end

    it 'has object nodes' do
      expect(subject).to have_node 'c'
    end

    it 'does not have nodes that are not present' do
      expect(subject).not_to have_node 'd'
    end

    it 'does not have nodes when nodes with similar beginnings are present' do
      subject.insert('dd', 'b', 'c')

      expect(subject).not_to have_node 'd'
    end

    it 'does not have predicate (non-)nodes' do
      expect(subject).not_to have_node 'b'
    end

    it 'when edge is also node, has predicate node' do
      subject.insert('b', 'b', 'c')

      expect(subject).to have_node 'b'
    end
    
    context 'in graph' do
      it 'is true for graph with node' do
        expect { subject.insert('a', 'b', 'c', graph: 'g') }
          .to change { subject.has_node?('a', graph: 'g') }.from(false).to(true)
      end
    end
  end

  describe '#has_edge?' do
    it 'is false when edge does not exist' do
      expect(subject).not_to have_edge('s', 'p', 'o')
    end

    describe 'with edges' do
      include_context 'with edges'

      it 'is true for existing statements' do
        expect(subject).to have_edge(*edges.first)
      end

      it 'is false for truncated key' do
        edge = edges.first

        expect(subject).not_to have_edge(edge[0], edge[1], edge[2][0..-2])
      end

      context 'in graph' do
        it 'is true for graph with node' do
          expect { subject.insert(*edges.first, graph: 'g') }
            .to change { subject.has_edge?(*edges.first, graph: 'g') }
                 .from(false).to(true)
        end
      end
    end
  end

  describe 'adjacent?' do
    it 'is false when edge does not exist' do
      expect(subject).not_to be_adjacent('s', 'o')
    end

    context 'with edges' do
      include_context 'with edges'
      
      let(:edge) { edges.first }

      it 'is true when edge exists' do
        expect(subject).to be_adjacent(edge.first, edge.last)
      end

      it 'is true when edge is reversed' do
        expect(subject).to be_adjacent(edge.last, edge.first)
      end

      it 'is false for truncated edges' do
        expect(subject).not_to be_adjacent(edge.first[0..-2], edge.last)
        expect(subject).not_to be_adjacent(edge.first, edge.last[0..-2])
      end
      
      context 'in graph' do
        it 'is not adjacent' do
          expect(subject).not_to be_adjacent(edge.last, edge.first, graph: 'g')
        end

        it 'is adjacent' do
          subject.insert(*edge, graph: 'g')
          expect(subject).to be_adjacent(edge.last, edge.first, graph: 'g')
        end
      end
    end
  end

  describe '#inserts' do
    let(:edges) { random_edges(5) }

    it 'inserts correct edges' do
      expect { subject.inserts(edges) }
        .to change { subject.edges }.to contain_exactly(*edges)
    end

    context 'in graph' do
      it 'inserts correct edges to graph' do
        expect { subject.inserts(edges, graph: 'g') }
          .to change { subject.edges(graph: 'g') }.to contain_exactly(*edges)
      end

      it 'does not insert to default graph' do
        expect { subject.inserts(edges, graph: 'g') }
          .not_to change { subject.edges.to_a }
      end

      it 'inserts to graph from statement array' do
        edges.first << 'g'

        expect { subject.inserts(edges) }
          .to change { subject.edges(graph: 'g') }
               .to contain_exactly(edges.first[0..2])
        
        expect(subject.edges).to contain_exactly(*edges[1..-1])
      end
    end
  end

  describe '#deletes' do
    include_context 'with edges'
    
    it 'deletes edges' do
      expect { subject.deletes(edges) }
        .to change { subject.count }.from(edges.count).to(0)
    end

    it 'deletes edges' do
      new_edges = random_edges(10)
      subject.inserts(new_edges)

      expect { subject.deletes(edges) }
        .to change { subject.edges.to_a }.to contain_exactly(*new_edges)
    end
  end

  describe '#count' do
    let(:edges) { random_edges(5) }

    it 'when empty gives 0' do
      expect(subject.count).to eq 0
    end

    it 'gives a current count' do
      expect { subject.inserts(edges) }
        .to change { subject.count }.from(0).to(edges.count)
    end
  end
end
