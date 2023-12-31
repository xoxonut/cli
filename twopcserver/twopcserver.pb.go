// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v3.12.4
// source: twopcserver.proto

package twopcserver

import (
	empty "github.com/golang/protobuf/ptypes/empty"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// if operation successed err is false and msg is "successed"
// of err is true and msg is the cause
type Response struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Msg string `protobuf:"bytes,1,opt,name=msg,proto3" json:"msg,omitempty"`
}

func (x *Response) Reset() {
	*x = Response{}
	if protoimpl.UnsafeEnabled {
		mi := &file_twopcserver_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Response) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Response) ProtoMessage() {}

func (x *Response) ProtoReflect() protoreflect.Message {
	mi := &file_twopcserver_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Response.ProtoReflect.Descriptor instead.
func (*Response) Descriptor() ([]byte, []int) {
	return file_twopcserver_proto_rawDescGZIP(), []int{0}
}

func (x *Response) GetMsg() string {
	if x != nil {
		return x.Msg
	}
	return ""
}

// if account has been created, fail
type CreateAccountRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AccountId int32 `protobuf:"varint,1,opt,name=account_id,json=accountId,proto3" json:"account_id,omitempty"`
}

func (x *CreateAccountRequest) Reset() {
	*x = CreateAccountRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_twopcserver_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateAccountRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateAccountRequest) ProtoMessage() {}

func (x *CreateAccountRequest) ProtoReflect() protoreflect.Message {
	mi := &file_twopcserver_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateAccountRequest.ProtoReflect.Descriptor instead.
func (*CreateAccountRequest) Descriptor() ([]byte, []int) {
	return file_twopcserver_proto_rawDescGZIP(), []int{1}
}

func (x *CreateAccountRequest) GetAccountId() int32 {
	if x != nil {
		return x.AccountId
	}
	return 0
}

// if account does not exsist, fail
type DeleteAccountRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AccountId int32 `protobuf:"varint,1,opt,name=account_id,json=accountId,proto3" json:"account_id,omitempty"`
}

func (x *DeleteAccountRequest) Reset() {
	*x = DeleteAccountRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_twopcserver_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DeleteAccountRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteAccountRequest) ProtoMessage() {}

func (x *DeleteAccountRequest) ProtoReflect() protoreflect.Message {
	mi := &file_twopcserver_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteAccountRequest.ProtoReflect.Descriptor instead.
func (*DeleteAccountRequest) Descriptor() ([]byte, []int) {
	return file_twopcserver_proto_rawDescGZIP(), []int{2}
}

func (x *DeleteAccountRequest) GetAccountId() int32 {
	if x != nil {
		return x.AccountId
	}
	return 0
}

// if account does not exsist, fail
type ReadAccountRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AccountId int32 `protobuf:"varint,1,opt,name=account_id,json=accountId,proto3" json:"account_id,omitempty"`
}

func (x *ReadAccountRequest) Reset() {
	*x = ReadAccountRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_twopcserver_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadAccountRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadAccountRequest) ProtoMessage() {}

func (x *ReadAccountRequest) ProtoReflect() protoreflect.Message {
	mi := &file_twopcserver_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadAccountRequest.ProtoReflect.Descriptor instead.
func (*ReadAccountRequest) Descriptor() ([]byte, []int) {
	return file_twopcserver_proto_rawDescGZIP(), []int{3}
}

func (x *ReadAccountRequest) GetAccountId() int32 {
	if x != nil {
		return x.AccountId
	}
	return 0
}

// set account's balance to the amount and the amonunt must >= 0
// if account does not exsist, fail
type UpdateAccountRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AccountId int32 `protobuf:"varint,1,opt,name=account_id,json=accountId,proto3" json:"account_id,omitempty"`
	Amount    int32 `protobuf:"varint,2,opt,name=amount,proto3" json:"amount,omitempty"`
}

func (x *UpdateAccountRequest) Reset() {
	*x = UpdateAccountRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_twopcserver_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UpdateAccountRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateAccountRequest) ProtoMessage() {}

func (x *UpdateAccountRequest) ProtoReflect() protoreflect.Message {
	mi := &file_twopcserver_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateAccountRequest.ProtoReflect.Descriptor instead.
func (*UpdateAccountRequest) Descriptor() ([]byte, []int) {
	return file_twopcserver_proto_rawDescGZIP(), []int{4}
}

func (x *UpdateAccountRequest) GetAccountId() int32 {
	if x != nil {
		return x.AccountId
	}
	return 0
}

func (x *UpdateAccountRequest) GetAmount() int32 {
	if x != nil {
		return x.Amount
	}
	return 0
}

//account balance must >= -amount
//if account does not exsist, fail
//if there is lock in 2pc, we must save locks in server
type BeginTransactionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AccountId int32  `protobuf:"varint,1,opt,name=account_id,json=accountId,proto3" json:"account_id,omitempty"`
	Amount    int32  `protobuf:"varint,2,opt,name=amount,proto3" json:"amount,omitempty"`
	Uuid      string `protobuf:"bytes,3,opt,name=uuid,proto3" json:"uuid,omitempty"`
}

func (x *BeginTransactionRequest) Reset() {
	*x = BeginTransactionRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_twopcserver_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BeginTransactionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BeginTransactionRequest) ProtoMessage() {}

func (x *BeginTransactionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_twopcserver_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BeginTransactionRequest.ProtoReflect.Descriptor instead.
func (*BeginTransactionRequest) Descriptor() ([]byte, []int) {
	return file_twopcserver_proto_rawDescGZIP(), []int{5}
}

func (x *BeginTransactionRequest) GetAccountId() int32 {
	if x != nil {
		return x.AccountId
	}
	return 0
}

func (x *BeginTransactionRequest) GetAmount() int32 {
	if x != nil {
		return x.Amount
	}
	return 0
}

func (x *BeginTransactionRequest) GetUuid() string {
	if x != nil {
		return x.Uuid
	}
	return ""
}

// do the saved operation and released locked account
type CommitRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AccountId int32  `protobuf:"varint,1,opt,name=account_id,json=accountId,proto3" json:"account_id,omitempty"`
	Amount    int32  `protobuf:"varint,2,opt,name=amount,proto3" json:"amount,omitempty"`
	Uuid      string `protobuf:"bytes,3,opt,name=uuid,proto3" json:"uuid,omitempty"`
}

func (x *CommitRequest) Reset() {
	*x = CommitRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_twopcserver_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CommitRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CommitRequest) ProtoMessage() {}

func (x *CommitRequest) ProtoReflect() protoreflect.Message {
	mi := &file_twopcserver_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CommitRequest.ProtoReflect.Descriptor instead.
func (*CommitRequest) Descriptor() ([]byte, []int) {
	return file_twopcserver_proto_rawDescGZIP(), []int{6}
}

func (x *CommitRequest) GetAccountId() int32 {
	if x != nil {
		return x.AccountId
	}
	return 0
}

func (x *CommitRequest) GetAmount() int32 {
	if x != nil {
		return x.Amount
	}
	return 0
}

func (x *CommitRequest) GetUuid() string {
	if x != nil {
		return x.Uuid
	}
	return ""
}

//do nothing but released locked accoount
type AbortRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	AccountId int32  `protobuf:"varint,1,opt,name=account_id,json=accountId,proto3" json:"account_id,omitempty"`
	Uuid      string `protobuf:"bytes,2,opt,name=uuid,proto3" json:"uuid,omitempty"`
}

func (x *AbortRequest) Reset() {
	*x = AbortRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_twopcserver_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AbortRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AbortRequest) ProtoMessage() {}

func (x *AbortRequest) ProtoReflect() protoreflect.Message {
	mi := &file_twopcserver_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AbortRequest.ProtoReflect.Descriptor instead.
func (*AbortRequest) Descriptor() ([]byte, []int) {
	return file_twopcserver_proto_rawDescGZIP(), []int{7}
}

func (x *AbortRequest) GetAccountId() int32 {
	if x != nil {
		return x.AccountId
	}
	return 0
}

func (x *AbortRequest) GetUuid() string {
	if x != nil {
		return x.Uuid
	}
	return ""
}

var File_twopcserver_proto protoreflect.FileDescriptor

var file_twopcserver_proto_rawDesc = []byte{
	0x0a, 0x11, 0x74, 0x77, 0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x74, 0x77, 0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x1c, 0x0a,
	0x08, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x6d, 0x73, 0x67,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6d, 0x73, 0x67, 0x22, 0x35, 0x0a, 0x14, 0x43,
	0x72, 0x65, 0x61, 0x74, 0x65, 0x41, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x5f, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x09, 0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74,
	0x49, 0x64, 0x22, 0x35, 0x0a, 0x14, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x41, 0x63, 0x63, 0x6f,
	0x75, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x61, 0x63,
	0x63, 0x6f, 0x75, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x09,
	0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x49, 0x64, 0x22, 0x33, 0x0a, 0x12, 0x52, 0x65, 0x61,
	0x64, 0x41, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x1d, 0x0a, 0x0a, 0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x09, 0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x49, 0x64, 0x22, 0x4d,
	0x0a, 0x14, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x41, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e,
	0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x09, 0x61, 0x63, 0x63, 0x6f,
	0x75, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x61, 0x6d, 0x6f, 0x75, 0x6e, 0x74, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x06, 0x61, 0x6d, 0x6f, 0x75, 0x6e, 0x74, 0x22, 0x64, 0x0a,
	0x17, 0x42, 0x65, 0x67, 0x69, 0x6e, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f,
	0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x61, 0x63, 0x63, 0x6f,
	0x75, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x09, 0x61, 0x63,
	0x63, 0x6f, 0x75, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x61, 0x6d, 0x6f, 0x75, 0x6e,
	0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x06, 0x61, 0x6d, 0x6f, 0x75, 0x6e, 0x74, 0x12,
	0x12, 0x0a, 0x04, 0x75, 0x75, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x75,
	0x75, 0x69, 0x64, 0x22, 0x5a, 0x0a, 0x0d, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x5f,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x09, 0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e,
	0x74, 0x49, 0x64, 0x12, 0x16, 0x0a, 0x06, 0x61, 0x6d, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x06, 0x61, 0x6d, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x75,
	0x75, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x75, 0x75, 0x69, 0x64, 0x22,
	0x41, 0x0a, 0x0c, 0x41, 0x62, 0x6f, 0x72, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x1d, 0x0a, 0x0a, 0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x09, 0x61, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x12,
	0x0a, 0x04, 0x75, 0x75, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x75, 0x75,
	0x69, 0x64, 0x32, 0xc0, 0x04, 0x0a, 0x15, 0x54, 0x77, 0x6f, 0x50, 0x68, 0x61, 0x73, 0x65, 0x43,
	0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x49, 0x0a, 0x0d,
	0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x41, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x21, 0x2e,
	0x74, 0x77, 0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x43, 0x72, 0x65, 0x61,
	0x74, 0x65, 0x41, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x15, 0x2e, 0x74, 0x77, 0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x49, 0x0a, 0x0d, 0x44, 0x65, 0x6c, 0x65, 0x74,
	0x65, 0x41, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x21, 0x2e, 0x74, 0x77, 0x6f, 0x70, 0x63,
	0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x41, 0x63, 0x63,
	0x6f, 0x75, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x15, 0x2e, 0x74, 0x77,
	0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x45, 0x0a, 0x0b, 0x52, 0x65, 0x61, 0x64, 0x41, 0x63, 0x63, 0x6f, 0x75, 0x6e,
	0x74, 0x12, 0x1f, 0x2e, 0x74, 0x77, 0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e,
	0x52, 0x65, 0x61, 0x64, 0x41, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x15, 0x2e, 0x74, 0x77, 0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
	0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x49, 0x0a, 0x0d, 0x55, 0x70, 0x64,
	0x61, 0x74, 0x65, 0x41, 0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x21, 0x2e, 0x74, 0x77, 0x6f,
	0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x41,
	0x63, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x15, 0x2e,
	0x74, 0x77, 0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x4f, 0x0a, 0x10, 0x42, 0x65, 0x67, 0x69, 0x6e, 0x54, 0x72, 0x61,
	0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x24, 0x2e, 0x74, 0x77, 0x6f, 0x70, 0x63,
	0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x42, 0x65, 0x67, 0x69, 0x6e, 0x54, 0x72, 0x61, 0x6e,
	0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x15,
	0x2e, 0x74, 0x77, 0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x3b, 0x0a, 0x06, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x12,
	0x1a, 0x2e, 0x74, 0x77, 0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x43, 0x6f,
	0x6d, 0x6d, 0x69, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x15, 0x2e, 0x74, 0x77,
	0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x39, 0x0a, 0x05, 0x41, 0x62, 0x6f, 0x72, 0x74, 0x12, 0x19, 0x2e, 0x74, 0x77,
	0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x41, 0x62, 0x6f, 0x72, 0x74, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x15, 0x2e, 0x74, 0x77, 0x6f, 0x70, 0x63, 0x73, 0x65,
	0x72, 0x76, 0x65, 0x72, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x36, 0x0a,
	0x05, 0x52, 0x65, 0x73, 0x65, 0x74, 0x12, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x1a, 0x15,
	0x2e, 0x74, 0x77, 0x6f, 0x70, 0x63, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2e, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x0f, 0x5a, 0x0d, 0x2e, 0x2f, 0x74, 0x77, 0x6f, 0x70, 0x63,
	0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_twopcserver_proto_rawDescOnce sync.Once
	file_twopcserver_proto_rawDescData = file_twopcserver_proto_rawDesc
)

func file_twopcserver_proto_rawDescGZIP() []byte {
	file_twopcserver_proto_rawDescOnce.Do(func() {
		file_twopcserver_proto_rawDescData = protoimpl.X.CompressGZIP(file_twopcserver_proto_rawDescData)
	})
	return file_twopcserver_proto_rawDescData
}

var file_twopcserver_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_twopcserver_proto_goTypes = []interface{}{
	(*Response)(nil),                // 0: twopcserver.Response
	(*CreateAccountRequest)(nil),    // 1: twopcserver.CreateAccountRequest
	(*DeleteAccountRequest)(nil),    // 2: twopcserver.DeleteAccountRequest
	(*ReadAccountRequest)(nil),      // 3: twopcserver.ReadAccountRequest
	(*UpdateAccountRequest)(nil),    // 4: twopcserver.UpdateAccountRequest
	(*BeginTransactionRequest)(nil), // 5: twopcserver.BeginTransactionRequest
	(*CommitRequest)(nil),           // 6: twopcserver.CommitRequest
	(*AbortRequest)(nil),            // 7: twopcserver.AbortRequest
	(*empty.Empty)(nil),             // 8: google.protobuf.Empty
}
var file_twopcserver_proto_depIdxs = []int32{
	1, // 0: twopcserver.TwoPhaseCommitService.CreateAccount:input_type -> twopcserver.CreateAccountRequest
	2, // 1: twopcserver.TwoPhaseCommitService.DeleteAccount:input_type -> twopcserver.DeleteAccountRequest
	3, // 2: twopcserver.TwoPhaseCommitService.ReadAccount:input_type -> twopcserver.ReadAccountRequest
	4, // 3: twopcserver.TwoPhaseCommitService.UpdateAccount:input_type -> twopcserver.UpdateAccountRequest
	5, // 4: twopcserver.TwoPhaseCommitService.BeginTransaction:input_type -> twopcserver.BeginTransactionRequest
	6, // 5: twopcserver.TwoPhaseCommitService.Commit:input_type -> twopcserver.CommitRequest
	7, // 6: twopcserver.TwoPhaseCommitService.Abort:input_type -> twopcserver.AbortRequest
	8, // 7: twopcserver.TwoPhaseCommitService.Reset:input_type -> google.protobuf.Empty
	0, // 8: twopcserver.TwoPhaseCommitService.CreateAccount:output_type -> twopcserver.Response
	0, // 9: twopcserver.TwoPhaseCommitService.DeleteAccount:output_type -> twopcserver.Response
	0, // 10: twopcserver.TwoPhaseCommitService.ReadAccount:output_type -> twopcserver.Response
	0, // 11: twopcserver.TwoPhaseCommitService.UpdateAccount:output_type -> twopcserver.Response
	0, // 12: twopcserver.TwoPhaseCommitService.BeginTransaction:output_type -> twopcserver.Response
	0, // 13: twopcserver.TwoPhaseCommitService.Commit:output_type -> twopcserver.Response
	0, // 14: twopcserver.TwoPhaseCommitService.Abort:output_type -> twopcserver.Response
	0, // 15: twopcserver.TwoPhaseCommitService.Reset:output_type -> twopcserver.Response
	8, // [8:16] is the sub-list for method output_type
	0, // [0:8] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_twopcserver_proto_init() }
func file_twopcserver_proto_init() {
	if File_twopcserver_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_twopcserver_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Response); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_twopcserver_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateAccountRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_twopcserver_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DeleteAccountRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_twopcserver_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReadAccountRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_twopcserver_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UpdateAccountRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_twopcserver_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BeginTransactionRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_twopcserver_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CommitRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_twopcserver_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AbortRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_twopcserver_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_twopcserver_proto_goTypes,
		DependencyIndexes: file_twopcserver_proto_depIdxs,
		MessageInfos:      file_twopcserver_proto_msgTypes,
	}.Build()
	File_twopcserver_proto = out.File
	file_twopcserver_proto_rawDesc = nil
	file_twopcserver_proto_goTypes = nil
	file_twopcserver_proto_depIdxs = nil
}
