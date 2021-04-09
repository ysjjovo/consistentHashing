package hashingCycle

import "hash/crc32"

//计算key对应的hash值
func Hash(key string) int {
	v := int(crc32.ChecksumIEEE([]byte(key)))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	return 0
}