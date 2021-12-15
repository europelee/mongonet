package mongonet

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestProxyProtocol(t *testing.T) {

	var (
		v2SignatureBytes = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

		// version/command.
		localBytes = []byte{0x20}
		proxyBytes = []byte{0x21}

		// address family and protocol
		tcpV4Bytes = []byte{0x11}
		udpV4Bytes = []byte{0x12}
		tcpV6Bytes = []byte{0x21}
		udpV6Bytes = []byte{0x22}

		// addresses
		localIPv4                 = net.ParseIP("127.0.0.1").To4()
		localIPv6                 = net.ParseIP("::1").To16()
		remoteIPv4                = net.ParseIP("127.0.0.2").To4()
		remoteIPv6                = net.ParseIP("::2").To16()
		portBytes                 = []byte{0xFD, 0xE8, 0xFD, 0xE9}
		v4AddressesBytes          = append(localIPv4, remoteIPv4...)
		v6AddressesBytes          = append(localIPv6, remoteIPv6...)
		v4AddressesWithPortsBytes = append(v4AddressesBytes, portBytes...)
		v6AddressesWithPortsBytes = append(v6AddressesBytes, portBytes...)

		// lengths
		lengthV4          = uint16(12)
		lengthV6          = uint16(36)
		lengthPadded      = uint16(84)
		lengthV4Bytes     = []byte{0x00, 0xC}
		lengthV6Bytes     = []byte{0x00, 0x24}
		lengthEmptyBytes  = []byte{0x00, 0x00}
		lengthPaddedBytes = []byte{0x00, 0x54}

		// private endpoint values
		emptyPrivateInfo = PrivateEndpointInfo{"", ""}

		awsPESubtypeBytes   = []byte{0x01}
		azurePESubtypeBytes = []byte{0x01}
		badPESubtypeBytes   = []byte{0x55}

		gcpType          = []byte{GCP_PE_UNIQUE_ID_TYPE}
		lengthGcpBytes   = []byte{0x00, 0x08}
		lengthV4GcpBytes = []byte{0x00, 0x17}
		lengthV6GcpBytes = []byte{0x00, 0x2F}
		gcpBytes         = []byte{0x03, 0x26, 0x18, 0x19, 0x20, 0x21, 0x22, 0x23}
		encodedGcpPE     = PrivateEndpointInfo{"4A==", "AyYYGSAhIiM="}

		awsType          = []byte{PP2_TYPE_AWS}
		lengthAwsBytes   = []byte{0x00, 0x07}
		lengthV4AwsBytes = []byte{0x00, 0x16}
		awsPEBytes       = []byte{0x04, 0x1A, 0x24, 0x55, 0x66, 0x77}
		awsBytes         = append(awsPESubtypeBytes, awsPEBytes...)
		encodedAwsPe     = PrivateEndpointInfo{"6g==", "BBokVWZ3"}

		azureType          = []byte{PP2_TYPE_AZURE}
		lengthAzureBytes   = []byte{0x00, 0x05}
		lengthV4AzureBytes = []byte{0x00, 0x14}
		azurePEBytes       = []byte{0x14, 0x15, 0x12, 0xA4}
		azureBytes         = append(azurePESubtypeBytes, azurePEBytes...)
		encodedAzurePe     = PrivateEndpointInfo{"7g==", "FBUSpA=="}

		// message
		messageBytes = []byte("MESSAGE")

		// util
		concat = func(parts ...[]byte) []byte {
			var result []byte
			for _, p := range parts {
				result = append(result, p...)
			}
			return result
		}
	)

	testCases := []struct {
		name                string
		bytes               []byte
		expectedLocalAddr   string
		expectedProxyAddr   string
		expectedRemoteAddr  string
		expectedTargetAddr  string
		expectedErr         error
		expectedPrivateInfo PrivateEndpointInfo
	}{
		{
			"not proxy protocol",
			messageBytes,
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
			emptyPrivateInfo,
		},
		{
			"v1-invalid header 1",
			[]byte("PROXY"),
			"",
			"",
			"",
			"",
			errors.New("invalid header"),
			emptyPrivateInfo,
		},
		{
			"v1-invalid header 2",
			[]byte("PROXY \r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid header"),
			emptyPrivateInfo,
		},
		{
			"v1-invalid header 2",
			[]byte("PROXY UDP4 127.0.0.1 127.0.0.2 65000 65001\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid protocol and family"),
			emptyPrivateInfo,
		},
		{
			"v1-invalid src ip address",
			[]byte("PROXY TCP4 127.0 127.0.0.2 65000 65001\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid ip address"),
			emptyPrivateInfo,
		},
		{
			"v1-invalid dst ip address",
			[]byte("PROXY TCP4 127.0.0.1 127.2 65000 65001\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid ip address"),
			emptyPrivateInfo,
		},
		{
			"v1-invalid src port",
			[]byte("PROXY TCP4 127.0.0.1 127.0.0.2 70000 65001\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid port number"),
			emptyPrivateInfo,
		},
		{
			"v1-invalid dst port",
			[]byte("PROXY TCP4 127.0.0.1 127.0.0.2 65000 abd\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid port number"),
			emptyPrivateInfo,
		},
		{
			"v1-unknown",
			[]byte("PROXY UNKNOWN 127.0.0.1 127.0.0.2 65000 65001\r\nMESSAGE"),
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
			emptyPrivateInfo,
		},
		{
			"v1-unknown",
			[]byte("PROXY UNKNOWN\r\nMESSAGE"),
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
			emptyPrivateInfo,
		},
		{
			"v1-tcp4",
			[]byte("PROXY TCP4 127.0.0.1 127.0.0.2 65000 65001\r\nMESSAGE"),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"v1-tcp6",
			[]byte("PROXY TCP6 ::1 ::2 65000 65001\r\nMESSAGE"),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"[::1]:65000",
			"[::2]:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-invalid version",
			concat(v2SignatureBytes, []byte{0x10}, tcpV4Bytes, lengthEmptyBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid version"),
			emptyPrivateInfo,
		},
		{
			"v2-invalid command",
			concat(v2SignatureBytes, []byte{0x22}, tcpV4Bytes, lengthEmptyBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid command"),
			emptyPrivateInfo,
		},
		{
			"v2-invalid address family",
			concat(v2SignatureBytes, proxyBytes, []byte{0x41}, lengthEmptyBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid address family"),
			emptyPrivateInfo,
		},
		{
			"v2-invalid protocol",
			concat(v2SignatureBytes, proxyBytes, []byte{0x23}, lengthV6Bytes, v6AddressesWithPortsBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid protocol"),
			emptyPrivateInfo,
		},
		{
			"v2-invalid ipv4 length",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, []byte{0x00, 0xB}, v4AddressesWithPortsBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid IPv4 payload"),
			emptyPrivateInfo,
		},
		{
			"v2-invalid ipv6 length",
			concat(v2SignatureBytes, proxyBytes, tcpV6Bytes, []byte{0x00, 0xC}, v4AddressesWithPortsBytes, messageBytes),
			"",
			"",
			"",
			"",
			errors.New("invalid IPv6 payload"),
			emptyPrivateInfo,
		},
		{
			"v2-local",
			concat(v2SignatureBytes, localBytes, tcpV4Bytes, lengthEmptyBytes, messageBytes),
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-local-tcp-ipv4 with extra bytes",
			concat(v2SignatureBytes, localBytes, tcpV4Bytes, lengthPaddedBytes, v4AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV4), messageBytes),
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-local-tcp-ipv6 with extra bytes",
			concat(v2SignatureBytes, localBytes, tcpV6Bytes, lengthPaddedBytes, v6AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV6), messageBytes),
			"192.168.0.2:5001",
			"",
			"192.168.0.1:5000",
			"",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-proxy-tcp-ipv4",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthV4Bytes, v4AddressesWithPortsBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-proxy-tcp-ipv4 with extra bytes",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthPaddedBytes, v4AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV4), messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-proxy-tcp-ipv6",
			concat(v2SignatureBytes, proxyBytes, tcpV6Bytes, lengthV6Bytes, v6AddressesWithPortsBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"[::1]:65000",
			"[::2]:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-proxy-tcp-ipv6 with extra bytes",
			concat(v2SignatureBytes, proxyBytes, tcpV6Bytes, lengthPaddedBytes, v6AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV6), messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"[::1]:65000",
			"[::2]:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-proxy-udp-ipv4",
			concat(v2SignatureBytes, proxyBytes, udpV4Bytes, lengthV4Bytes, v4AddressesWithPortsBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-proxy-udp-ipv4 with extra bytes",
			concat(v2SignatureBytes, proxyBytes, udpV4Bytes, lengthPaddedBytes, v4AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV4), messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-proxy-udp-ipv6",
			concat(v2SignatureBytes, proxyBytes, udpV6Bytes, lengthV6Bytes, v6AddressesWithPortsBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"[::1]:65000",
			"[::2]:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-proxy-udp-ipv6 with extra bytes",
			concat(v2SignatureBytes, proxyBytes, udpV6Bytes, lengthPaddedBytes, v6AddressesWithPortsBytes, make([]byte, lengthPadded-lengthV6), messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"[::1]:65000",
			"[::2]:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"unix sockets",
			[]byte("\r\n\r\n\x00\r\nQUIT\n!0\x00\x00"),
			"",
			"",
			"",
			"",
			errors.New("unix sockets are not supported"),
			emptyPrivateInfo,
		},
		{
			"not enough data",
			[]byte("PROXY\r\n"),
			"",
			"",
			"",
			"",
			errors.New("invalid header"),
			emptyPrivateInfo,
		},
		{
			"v2-proxy-tcp-ipv4 gcp PE",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthV4GcpBytes, v4AddressesWithPortsBytes, gcpType, lengthGcpBytes, gcpBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
			encodedGcpPE,
		},
		{
			"v2-proxy-tcp-ipv6 gcp PE",
			concat(v2SignatureBytes, proxyBytes, tcpV6Bytes, lengthV6GcpBytes, v6AddressesWithPortsBytes, gcpType, lengthGcpBytes, gcpBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"[::1]:65000",
			"[::2]:65001",
			nil,
			encodedGcpPE,
		},
		{
			"v2-proxy-tcp-ipv4 unused TLV + gcp PE",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, []byte{0x00, 0x1D}, v4AddressesWithPortsBytes, []byte{0x55}, []byte{0x00, 0x03}, []byte{0x33, 0xAA, 0xA4}, gcpType, lengthGcpBytes, gcpBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
			encodedGcpPE,
		},
		{
			"v2-proxy-tcp-ipv4 gcp PE wrong length",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthV4GcpBytes, v4AddressesWithPortsBytes, gcpType, lengthAzureBytes, azureBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			errors.New("unexpected length 5 found for GCP_PE_UNIQUE_ID_TYPE TLV, expected 8"),
			emptyPrivateInfo,
		},
		{
			"v2-proxy-tcp-ipv4 aws PE",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthV4AwsBytes, v4AddressesWithPortsBytes, awsType, lengthAwsBytes, awsBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
			encodedAwsPe,
		},
		{
			"v2-proxy-tcp-ipv4 aws PE wrong subtype",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthV4AwsBytes, v4AddressesWithPortsBytes, awsType, lengthAwsBytes, badPESubtypeBytes, awsPEBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-proxy-tcp-ipv4 aws PE zero length",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthV4AwsBytes, v4AddressesWithPortsBytes, awsType, []byte{0x00, 0x00}, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			errors.New("unexpected 0 length found for PP2_TYPE_AWS TVL, unable to read subtype"),
			emptyPrivateInfo,
		},
		{
			"v2-proxy-tcp-ipv4 azure PE",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthV4AzureBytes, v4AddressesWithPortsBytes, azureType, lengthAzureBytes, azureBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
			encodedAzurePe,
		},
		{
			"v2-proxy-tcp-ipv4 azure PE wrong subtype",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthV4AzureBytes, v4AddressesWithPortsBytes, azureType, lengthAzureBytes, badPESubtypeBytes, azurePEBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			nil,
			emptyPrivateInfo,
		},
		{
			"v2-proxy-tcp-ipv4 azure PE wrong length",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthV4AwsBytes, v4AddressesWithPortsBytes, azureType, lengthAwsBytes, azurePESubtypeBytes, awsPEBytes, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			errors.New("unexpected length 7 found for PP2_TYPE_AZURE TLV subtype 0x01, expected 5"),
			emptyPrivateInfo,
		},
		{
			"v2-proxy-tcp-ipv4 azure PE zero length",
			concat(v2SignatureBytes, proxyBytes, tcpV4Bytes, lengthV4AwsBytes, v4AddressesWithPortsBytes, azureType, []byte{0x00, 0x00}, messageBytes),
			"192.168.0.2:5001",
			"192.168.0.1:5000",
			"127.0.0.1:65000",
			"127.0.0.2:65001",
			errors.New("unexpected 0 length found for PP2_TYPE_AZURE TVL, unable to read subtype"),
			emptyPrivateInfo,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := newMockConn(tc.bytes)
			subject, err := NewConn(c)
			if err != nil && tc.expectedErr == nil {
				t.Fatalf("expected no error, but got %v", err)
			} else if err == nil && tc.expectedErr != nil {
				t.Fatalf("expected error %v, but got none", tc.expectedErr)
			} else if err != nil && tc.expectedErr != nil {
				if err.Error() != tc.expectedErr.Error() {
					t.Fatalf("expected error %v, but got %v", tc.expectedErr, err)
				}
				return // we are successful
			}

			if tc.expectedLocalAddr != subject.LocalAddr().String() {
				t.Fatalf("expected local address %v, but got %v", tc.expectedLocalAddr, subject.LocalAddr())
			}

			if subject.IsProxied() && tc.expectedProxyAddr != subject.ProxyAddr().String() {
				t.Fatalf("expected proxy address %v, but got %v", tc.expectedProxyAddr, subject.ProxyAddr())
			}

			if tc.expectedRemoteAddr != subject.RemoteAddr().String() {
				t.Fatalf("expected remote address %v, but got %v", tc.expectedRemoteAddr, subject.RemoteAddr())
			}

			if subject.IsProxied() && tc.expectedTargetAddr != subject.TargetAddr().String() {
				t.Fatalf("expected target address %v, but got %v", tc.expectedTargetAddr, subject.TargetAddr())
			}

			if tc.expectedPrivateInfo != subject.PrivateEndpointInfo() {
				t.Fatalf("expected target private endpoint id %v, but got %v", tc.expectedPrivateInfo, subject.PrivateEndpointInfo())
			}

			actualMessageBytes := make([]byte, len(messageBytes))
			_, err = io.ReadFull(subject, actualMessageBytes)
			if err != nil {
				t.Fatalf("expected no error, but got %v", err)
			}
			if !bytes.Equal(messageBytes, actualMessageBytes) {
				t.Fatalf("expected message %v, but got %v", messageBytes, actualMessageBytes)
			}
		})
	}
}

func newMockConn(b []byte) *mockConn {
	return &mockConn{
		r: bytes.NewReader(b),
	}
}

type mockConn struct {
	r *bytes.Reader
}

func (c *mockConn) Close() error {
	return nil
}

func (c *mockConn) Read(b []byte) (n int, err error) {
	return c.r.Read(b)
}

func (c *mockConn) Write(b []byte) (n int, err error) {
	panic("don't call me")
}

func (c *mockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{
		IP:   net.ParseIP("192.168.0.2"),
		Port: 5001,
	}
}

func (c *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{
		IP:   net.ParseIP("192.168.0.1"),
		Port: 5000,
	}
}

func (c *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}
