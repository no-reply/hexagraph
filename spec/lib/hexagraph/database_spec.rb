require 'spec_helper'
require 'fileutils'

describe Hexagraph::Database do
  subject { described_class.new('.tmp/spec_db') }

  after(:context) do
    FileUtils.rm Dir.glob('.tmp/spec_*/*.mdb')
    FileUtils.rmdir Dir.glob('.tmp/spec_*')
  end

  describe 'initializing' do
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

  describe '#insert' do
    before { subject.clear }
    after { subject.clear }
      
    it 'inserts an edge' do
      expect { subject.insert('a', 'b', 'c') }
        .to change { subject.count }.from(0).to(1)
    end

    it 'has the edge after insert' do
    end

    it 'adds a triple' do
    end

    it 'does not re-insert' do
      subject.insert('a', 'b', 'c')
      expect { subject.insert('a', 'b', 'c') }
        .not_to change { subject.count }
    end
  end

  describe '#delete' do
    before { subject.insert('a', 'b', 'c') }
    after { subject.clear }

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
    after { subject.clear }

    it 'has subject nodes' do
      expect(subject).to have_node 'a'
    end

    it 'has object nodes' do
      expect(subject).to have_node 'c'
    end

    it 'does not have nodes that are not present' do
      expect(subject).not_to have_node 'd'
    end

    it 'does not have nodes that are not present' do
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
  end


  describe '#count' do
    it 'when empty gives 0' do
      expect(subject.count).to eq 0
    end

    context 'with statements' do
      it 'gives a current count' do
      end
    end
  end
end
