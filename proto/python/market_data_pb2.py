# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: market_data.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2

from google.protobuf.timestamp_pb2 import *

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x11market_data.proto\x12\x11Proto3.MarketData\x1a\x1fgoogle/protobuf/timestamp.proto\",\n\x0bPriceVolume\x12\r\n\x05price\x18\x01 \x01(\t\x12\x0e\n\x06volume\x18\x02 \x01(\t\"\xf9\x01\n\x05\x44\x65pth\x12-\n\ttimestamp\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x10\n\x08\x65xchange\x18\x02 \x01(\t\x12\x0e\n\x06symbol\x18\x03 \x01(\t\x12,\n\x04\x61sks\x18\x04 \x03(\x0b\x32\x1e.Proto3.MarketData.PriceVolume\x12,\n\x04\x62ids\x18\x05 \x03(\x0b\x32\x1e.Proto3.MarketData.PriceVolume\x12\x31\n\rmpu_timestamp\x18\x06 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x10\n\x08sequence\x18\x07 \x01(\x04\"\x89\x01\n\x05Trade\x12-\n\ttimestamp\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x10\n\x08\x65xchange\x18\x02 \x01(\t\x12\x0e\n\x06symbol\x18\x03 \x01(\t\x12\r\n\x05price\x18\x04 \x01(\t\x12\x0e\n\x06volume\x18\x05 \x01(\t\x12\x10\n\x08sequence\x18\x06 \x01(\x04\"\xe9\x01\n\x05Kline\x12-\n\ttimestamp\x18\x01 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x10\n\x08\x65xchange\x18\x02 \x01(\t\x12\x0e\n\x06symbol\x18\x03 \x01(\t\x12\x0c\n\x04open\x18\x04 \x01(\t\x12\x0c\n\x04high\x18\x05 \x01(\t\x12\x0b\n\x03low\x18\x06 \x01(\t\x12\r\n\x05\x63lose\x18\x07 \x01(\t\x12\x0e\n\x06volume\x18\x08 \x01(\t\x12\r\n\x05value\x18\t \x01(\t\x12\x12\n\nresolution\x18\n \x01(\r\x12\x10\n\x08sequence\x18\x0b \x01(\x04\x12\x12\n\nlastvolume\x18\x0c \x01(\t\"\xa7\x01\n\rHistKlineData\x12\x0e\n\x06symbol\x18\x01 \x01(\t\x12\x10\n\x08\x65xchange\x18\x02 \x01(\t\x12\x12\n\nstart_time\x18\x03 \x01(\x04\x12\x10\n\x08\x65nd_time\x18\x04 \x01(\x04\x12\r\n\x05\x63ount\x18\x05 \x01(\r\x12\x11\n\tfrequency\x18\x06 \x01(\r\x12,\n\nkline_data\x18\x07 \x03(\x0b\x32\x18.Proto3.MarketData.Kline\"\n\n\x08\x45mptyReq\"\n\n\x08\x45mptyRsp\"|\n\x10ReqHishKlineInfo\x12\x0e\n\x06symbol\x18\x01 \x01(\t\x12\x10\n\x08\x65xchange\x18\x02 \x01(\t\x12\x12\n\nstart_time\x18\x03 \x01(\x04\x12\x10\n\x08\x65nd_time\x18\x04 \x01(\x04\x12\r\n\x05\x63ount\x18\x05 \x01(\r\x12\x11\n\tfrequency\x18\x06 \x01(\r\"s\n\x0cReqTradeInfo\x12\x0e\n\x06symbol\x18\x01 \x01(\t\x12\x10\n\x08\x65xchange\x18\x02 \x01(\t\x12\x0c\n\x04time\x18\x03 \x01(\x04\x12\x12\n\nstart_time\x18\x05 \x01(\x04\x12\x10\n\x08\x65nd_time\x18\x06 \x01(\x04\x12\r\n\x05\x63ount\x18\x07 \x01(\r2\xc1\x01\n\rMarketService\x12_\n\x14RequestHistKlineData\x12#.Proto3.MarketData.ReqHishKlineInfo\x1a .Proto3.MarketData.HistKlineData\"\x00\x12O\n\x10RequestTradeData\x12\x1f.Proto3.MarketData.ReqTradeInfo\x1a\x18.Proto3.MarketData.Trade\"\x00\x42\x06Z\x04./pbP\x00\x62\x06proto3')



_PRICEVOLUME = DESCRIPTOR.message_types_by_name['PriceVolume']
_DEPTH = DESCRIPTOR.message_types_by_name['Depth']
_TRADE = DESCRIPTOR.message_types_by_name['Trade']
_KLINE = DESCRIPTOR.message_types_by_name['Kline']
_HISTKLINEDATA = DESCRIPTOR.message_types_by_name['HistKlineData']
_EMPTYREQ = DESCRIPTOR.message_types_by_name['EmptyReq']
_EMPTYRSP = DESCRIPTOR.message_types_by_name['EmptyRsp']
_REQHISHKLINEINFO = DESCRIPTOR.message_types_by_name['ReqHishKlineInfo']
_REQTRADEINFO = DESCRIPTOR.message_types_by_name['ReqTradeInfo']
PriceVolume = _reflection.GeneratedProtocolMessageType('PriceVolume', (_message.Message,), {
  'DESCRIPTOR' : _PRICEVOLUME,
  '__module__' : 'market_data_pb2'
  # @@protoc_insertion_point(class_scope:Proto3.MarketData.PriceVolume)
  })
_sym_db.RegisterMessage(PriceVolume)

Depth = _reflection.GeneratedProtocolMessageType('Depth', (_message.Message,), {
  'DESCRIPTOR' : _DEPTH,
  '__module__' : 'market_data_pb2'
  # @@protoc_insertion_point(class_scope:Proto3.MarketData.Depth)
  })
_sym_db.RegisterMessage(Depth)

Trade = _reflection.GeneratedProtocolMessageType('Trade', (_message.Message,), {
  'DESCRIPTOR' : _TRADE,
  '__module__' : 'market_data_pb2'
  # @@protoc_insertion_point(class_scope:Proto3.MarketData.Trade)
  })
_sym_db.RegisterMessage(Trade)

Kline = _reflection.GeneratedProtocolMessageType('Kline', (_message.Message,), {
  'DESCRIPTOR' : _KLINE,
  '__module__' : 'market_data_pb2'
  # @@protoc_insertion_point(class_scope:Proto3.MarketData.Kline)
  })
_sym_db.RegisterMessage(Kline)

HistKlineData = _reflection.GeneratedProtocolMessageType('HistKlineData', (_message.Message,), {
  'DESCRIPTOR' : _HISTKLINEDATA,
  '__module__' : 'market_data_pb2'
  # @@protoc_insertion_point(class_scope:Proto3.MarketData.HistKlineData)
  })
_sym_db.RegisterMessage(HistKlineData)

EmptyReq = _reflection.GeneratedProtocolMessageType('EmptyReq', (_message.Message,), {
  'DESCRIPTOR' : _EMPTYREQ,
  '__module__' : 'market_data_pb2'
  # @@protoc_insertion_point(class_scope:Proto3.MarketData.EmptyReq)
  })
_sym_db.RegisterMessage(EmptyReq)

EmptyRsp = _reflection.GeneratedProtocolMessageType('EmptyRsp', (_message.Message,), {
  'DESCRIPTOR' : _EMPTYRSP,
  '__module__' : 'market_data_pb2'
  # @@protoc_insertion_point(class_scope:Proto3.MarketData.EmptyRsp)
  })
_sym_db.RegisterMessage(EmptyRsp)

ReqHishKlineInfo = _reflection.GeneratedProtocolMessageType('ReqHishKlineInfo', (_message.Message,), {
  'DESCRIPTOR' : _REQHISHKLINEINFO,
  '__module__' : 'market_data_pb2'
  # @@protoc_insertion_point(class_scope:Proto3.MarketData.ReqHishKlineInfo)
  })
_sym_db.RegisterMessage(ReqHishKlineInfo)

ReqTradeInfo = _reflection.GeneratedProtocolMessageType('ReqTradeInfo', (_message.Message,), {
  'DESCRIPTOR' : _REQTRADEINFO,
  '__module__' : 'market_data_pb2'
  # @@protoc_insertion_point(class_scope:Proto3.MarketData.ReqTradeInfo)
  })
_sym_db.RegisterMessage(ReqTradeInfo)

_MARKETSERVICE = DESCRIPTOR.services_by_name['MarketService']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  DESCRIPTOR._serialized_options = b'Z\004./pb'
  _PRICEVOLUME._serialized_start=73
  _PRICEVOLUME._serialized_end=117
  _DEPTH._serialized_start=120
  _DEPTH._serialized_end=369
  _TRADE._serialized_start=372
  _TRADE._serialized_end=509
  _KLINE._serialized_start=512
  _KLINE._serialized_end=745
  _HISTKLINEDATA._serialized_start=748
  _HISTKLINEDATA._serialized_end=915
  _EMPTYREQ._serialized_start=917
  _EMPTYREQ._serialized_end=927
  _EMPTYRSP._serialized_start=929
  _EMPTYRSP._serialized_end=939
  _REQHISHKLINEINFO._serialized_start=941
  _REQHISHKLINEINFO._serialized_end=1065
  _REQTRADEINFO._serialized_start=1067
  _REQTRADEINFO._serialized_end=1182
  _MARKETSERVICE._serialized_start=1185
  _MARKETSERVICE._serialized_end=1378
# @@protoc_insertion_point(module_scope)
