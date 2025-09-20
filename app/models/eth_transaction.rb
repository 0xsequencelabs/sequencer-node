class EthTransaction < T::Struct
  include SysConfig
  
  const :block_hash, Hash32
  const :block_number, Integer
  const :block_timestamp, Integer
  const :tx_hash, Hash32
  const :transaction_index, Integer
  const :input, ByteString
  const :value, Integer
  const :chain_id, T.nilable(Integer)
  const :from_address, Address20
  const :to_address, T.nilable(Address20)
  const :status, Integer
  const :logs, T::Array[T.untyped], default: []
  const :eth_block, T.nilable(EthBlock)
  const :facet_transactions, T::Array[FacetTransaction], default: []
  
  FACET_INBOX_ADDRESS     = Address20.from_hex(ENV.fetch('INBOX_ADDRESS', "0x00000000000000000000000000000000000face7"))
  INBOX_FEE_WEI           = Integer(ENV.fetch('FEE_WEI', '0'))
  FacetLogInboxEventSig   = ByteString.from_hex(ENV.fetch('FACET_EVENT_TOPIC', "0x00000000000000000000000000000000000000000000000000000000000face7"))
  PaymentReceiptTopic0    = ByteString.from_hex(ENV.fetch('PAYMENT_RECEIPT_TOPIC0', "0x0"))


  sig { params(block_result: T.untyped, receipt_result: T.untyped).returns(T::Array[EthTransaction]) }
  def self.from_rpc_result(block_result, receipt_result)
    block_hash = block_result['hash']
    block_number = block_result['number'].to_i(16)
    
    indexed_receipts = receipt_result.index_by{|el| el['transactionHash']}
    
    block_result['transactions'].map do |tx|
      current_receipt = indexed_receipts[tx['hash']]
      
      EthTransaction.new(
        block_hash: Hash32.from_hex(block_hash),
        block_number: block_number,
        block_timestamp: block_result['timestamp'].to_i(16),
        tx_hash: Hash32.from_hex(tx['hash']),
        transaction_index: tx['transactionIndex'].to_i(16),
        input: ByteString.from_hex(tx['input']),
        chain_id: tx['chainId']&.to_i(16),
        from_address: Address20.from_hex(tx['from']),
        to_address: tx['to'] ? Address20.from_hex(tx['to']) : nil,
        status: current_receipt['status'].to_i(16),
        logs: current_receipt['logs'],
        value: tx['value'].to_i(16),
      )
    end
  end
  
  sig { params(block_results: T.untyped, receipt_results: T.untyped).returns(T::Array[T.untyped]) }
  def self.facet_txs_from_rpc_results(block_results, receipt_results)
    eth_txs = from_rpc_result(block_results, receipt_results)
    eth_txs.sort_by(&:transaction_index).map(&:to_facet_tx).compact
  end
  
  sig { returns(T.nilable(FacetTransaction)) }
  def to_facet_tx
    return unless is_success?
    
    facet_tx_from_input || try_facet_tx_from_events
  end
  
  sig { returns(T.nilable(FacetTransaction)) }
  def facet_tx_from_input
    return unless to_address == FACET_INBOX_ADDRESS
    return unless value == INBOX_FEE_WEI
    
    FacetTransaction.from_payload(
      contract_initiated: false,
      from_address: from_address,
      eth_transaction_input: input,
      tx_hash: tx_hash
    )
  end
  
  sig { returns(T.nilable(FacetTransaction)) }
  def try_facet_tx_from_events
    facet_tx_creation_events.each do |log|
      payload_bs  = ByteString.from_hex(log['data'])
      payload_hash = payload_bs.keccak256.to_hex
      if ENV.fetch('ENFORCE_EVENT_RECEIPT', 'true') == 'true'
        next unless has_inbox_payment_receipt?(payload_hash)
      end
      facet_tx = FacetTransaction.from_payload(
        contract_initiated: true,
        from_address: Address20.from_hex(log['address']),
        eth_transaction_input: payload_bs,
        tx_hash: tx_hash
      )
      return facet_tx if facet_tx
    end
    nil
  end
  
  sig { returns(T::Boolean) }
  def is_success?
    status == 1
  end
  
  sig { params(expected_payload_hash_hex: T.nilable(String)).returns(T::Boolean) }
  def has_inbox_payment_receipt?(expected_payload_hash_hex)
    logs.any? do |l|
      next false if l['removed'] || !l['topics'] || l['topics'].empty?

      begin
        addr_ok = Address20.from_hex(l['address']) == FACET_INBOX_ADDRESS
      rescue
        addr_ok = false
      end
      next false unless addr_ok

      topic0 = ByteString.from_hex(l['topics'].first)
      next false unless topic0 == PaymentReceiptTopic0

      data_hex = l['data']&.start_with?('0x') ? l['data'][2..-1] : l['data']
      return false unless data_hex && data_hex.length >= 64*2

      words = data_hex.scan(/.{1,64}/)
      amount_hex        = '0x' + (words.length >= 3 ? words[-2] : words[0])
      payload_hash_hex  = '0x' + (words.length >= 3 ? words[-1] : words[1])

      amount     = amount_hex.to_i(16)
      ok_amount  = (amount == INBOX_FEE_WEI)
      ok_payload = expected_payload_hash_hex.nil? || (payload_hash_hex.downcase == expected_payload_hash_hex.downcase)

      ok_amount && ok_payload
    end
  end

  def facet_tx_creation_events
    logs.select do |log|
      !log['removed'] && log['topics'].length == 1 &&
        FacetLogInboxEventSig == ByteString.from_hex(log['topics'].first)
    end.sort_by { |log| log['logIndex'].to_i(16) }
  end
  
  sig { returns(Hash32) }
  def facet_tx_source_hash
    tx_hash
  end
end
