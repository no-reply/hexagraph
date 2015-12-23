require 'spec_helper'

describe Hexagraph::Dictionary do
  after(:context) do
    FileUtils.rm Dir.glob('.tmp/spec_*/*.mdb')
    FileUtils.rmdir Dir.glob('.tmp/spec_*')
  end

  subject { described_class.new(env) }
  after { subject.clear! }

  let(:env) do
    FileUtils::mkdir_p '.tmp/dict_spec'
    LMDB.new('.tmp/dict_spec')
  end

  describe '#get' do
    it 'adds key to dictionary if not present' do
      str = 'moomin'
      expect(subject.get(str)).not_to be_nil
    end

    it 'fetches key from dictionary' do
      db = env.database('dict')
      db.put('k', 'v')
      expect(subject.get('k')).to eq 'v'
    end

    it 'uses different keys' do
      ids = (1..10).each_with_object([]) do |_, arry|
        k = (0...10).map { (65 + rand(26)).chr }.join
        arry << subject.get(k)
      end

      expect(ids.detect{ |e| ids.count(e) > 1 }).to be_nil
    end
  end

  describe '#lookup' do
    it 'performs id lookup ' do
      expect(subject.lookup(subject.get('moomin'))).to eq 'moomin'
    end
  end
end
