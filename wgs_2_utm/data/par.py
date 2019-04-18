# -*- coding: utf-8 -*-
# @Time    : 2019-04-04 18:09
# @Author  : liuyanming
# @Email   : 1420743191@qq.com
# @File    : par.py
# @Software: PyCharm


##python3

from typing import List, Tuple, Iterable

r1: str = '533f713e2bc4aed1dfba83b2e5007c37d0c698994117a668edbbc712afe20e8cb3c0a8821757c0d8ecd5a3cb6b2329a1e5d16b99cfa491693ab72a8a6574c27b4bb7ac331a77b559deac47bb88f5e60c0d59d21970a7593f805d5bfcca6a4e62467344f8943bcb0bb2788a178e4d5c719b3be14385570ebee6968d0a17f8afc7b397a8577bd3d5d55b88528bb2b9f08a78ff2d1c1e8a16bf1a1aad7e3663a2d42b772c1417eebeedacd4644be0dbced0f63cf5c911579beaa64a08511e1ce7a23fc0fee78a8845'

r2: str = '5437793920c4a6828dbed7acf80b7c72d2d282930410bd6ef1fc8e07e0f10e90b9dbf8821b1cd290e1c3af857e6c24aae5d777ddcaa3967e63fe29d87e7d876a5df8b1711a66a24a9bb355bb88f5ee164a53934c12b64271c9460fb4cd7359625f795faf8b2b9f1cfd668a44825f0e669c3bf354ca480fedf3929b0655f6bfdeb594e14b6996d39d55945496e8ea8bc276f92d26579a59f6005ba574282ea7cf3175695c15fea9eae5d56518bd8ffad8fc3ab78e485980aee250081f0448e5b53995fef28a8e0789770c84cf470202183ce528e9748961'

r3: str = '587a70313a81ef908da898b5f3006a71d5d8d78c4107a762ebfadc18aff4468ffcd8e781154f8599fa90becd692321a1a6d17494c8add06b2ce42e996632ca6951b4fe7e5476ed49deac52e99de9e20b0d4fc81870a25828d34112facc3c5f2a536844fc8e379f05fc659817cb650964963bf3548f4941a1e895954f59feebccb58bb6457dd2878153da4080a3a3b2cd37f87f08129042ba0d14ee542d6bb9d92b7a205a1dabbef5b6df235fa19efc9cec3dad864859cea8b7430013181cf3b32492efe186cc0a9e320ec781530f050f26a415a07e8a3fec5f73970b32f3cbc3e7e26b0ea3b022'

r4: str = '593b6e392283ef9bd8ac83fbf4007472c2c6968b4100f56ae6bb9f51bfb31e94b494ea870c48cd9cefc9ea8d656d68ada4cd7cddd2a2827e26be76d84335ca2857a8bb6d5b66a454dcfc52bb90f4f30c41459d0e39b71638c90913fdd86844304b3c09e08237914ed77c8a16920c417c907eb158840700baef9392431bb9bbcfb589ad412fdec68359da5296adafb88a7ae82d1b18de44b31755bc757b7da4cd3a3226525ae6a2b9a8df6e57bc92eacfa53cbfc918599dbee24010130448f3'

r5: str = '5c356b246c8ba9d1d9b792fbe7177770d2d59a8c0405a762bffada41e2ec5d94fcd5a88a1146c096aedfb8857f6c68bfa4d97c8e86a69f7524b97aac6277de2851b6bd734f76a81adea443f48ff4f311424ece4c3fa5163cc64702b4c47a0b2f4b3c02ee903dcd07e66fcf05874b41609a6ff95c990b41a4e999925353f0a5cdfa8da9412ffed29b5b9b418ca7a4fcc772f9650013de50b9061aba793e2eaad32c7b2e5a17eeb5ede5ca7157ac97ead1'

r6: str = '5e347d702382ef85c5bad7bff2097170c8c084df4b02f550f6f0c711eae74781fcddfbce0a54c48caed9bed62c6121a0a2cc788dcea3956863f03f966f60c66454a1fe6d5f64a85bd7fc52bb8cf8f50b424e9a1f70a5433dcb091afacf3c482d5f6c08ea92379f00f3678a48cb4540719f6ef558844041b9ef9fde4558ebb9cfb98de1536ecf878153da4095a3a6b08a7ef92d0619de52bf125cab633e60bf803e7e395c1be9beedb69a6256aadbfcdff73aa99d1b16'

r7: str = '5d3b6c353ec8ef86c5ba99fbde456c6ed0d1849a5044a16ffabbc70febe656c0a8dba89a1659858bebd3a5cb68232dabacca7092c8ea9f7d63c135947f7fc2280af4fe6a497ba35d9bbd5dbb99fcf5145400cd1e3fb75925de591eb4c47a0b16574444e688728e57aa3ac344a20c4673973be5598f0700afee9697524eb9bfc5fa90af4763c3c3901cb95b8ca8afafcf37ec630b57b457a61554ab623e2ea5c132773a1413e5fbedaddf6a4aee95eec8ec25bcc90e579ca7ec'

r8: str = '503438352081ac85dfb099b2f4457e78d2d9d7904244a16ff6e88e03e0ec45c0bfdbe58b0d1cc38aebd5ead2657720efb1d67cdde59db55963e4238b7e77ca2418baab6b1a66a55f9bac41f292e9e21c0d56d81e23aa593f874a14e7df6f0b2e576f17af923ade00b27e8701cb5c5c7b907eb15e8c070dacf49f8c5645f0a5deb397a6046ed8c3d5549b40c5a7eab2c374e82d0c188853a4545ba0757b6ca2ce3b7b275354'

r9: str = '5c3b76296c9daa90dfacd7acf20b6c37c2cddbdf4517f56ae6bbda08e2e60e97bdc7a88d1152d68de3d5ae856e7a68a0b1d67c8f86ba827429f2398c793c87515dacfe6b5277ed54d4a85af492bde81e0d72d81a35af5725ce4615b9c272062f476f0decc621cb0feb6f8b449c455a7ad376f41dca460fa9a7b3de405ef7aac6b680e1466ad1c69b1c8e5cc5b4afbdc67ef7684f039657a25473ee793a6aebc23a663d5108abbcfcb19a704caf89fbd9e1'

r10: str = '5e3c38332391bd82c8ffbefbfa046137c6d59e930410ba73fef7c218a3a34f8eb894e980071cc88dfdd9a98545233fbdacca7cddcba3977337b7389d2a70c67b51bbbf73566bed4dd4ae47f390f8f40b0300ff1924ef1630d40932b4d87d42261e3c17e08b37cb06fb648844834d5d32917ef45fca5213aaee9499065afcebdeb5d9a54b2fc2cf9c4fda558ab4eabd8a7be26308578a5fbb1116ee70356aebe97f74205a1eabaff1a4ce2371ee98eed2eb3cadc91a5d9da3b15146021845e9a92cce'

target: str = '45327d702185a69f8db693bef645716480c098df5601b266edff8e00aff35c8fbbc6e9835e5dd6d8ef90a9ca616e3da1acdd7889cfa59e3b37f87a907f7fc66618babb765475be1ac9bd47f399efa70c4541d34c31b01630875a1ee08b734d625b7217fb9427dc1afb658117cb584132923bf25e875714b9e288d00655e0ebeeb597a0486b96e2db1cb15d90b2a2'


def count_chr(c: int) -> bool:
    assert isinstance(c, int)
    """
    c == 0: ' '
    c in range(65, 91): [A-Z]
    c in range(97, 123): [a-z]
    """
    return c == 0 or c in range(65, 91) or c in range(97, 123)


def xor_2_bytes(r1: bytes, r2: bytes) -> bytes:
    assert isinstance(r1, bytes) and isinstance(r2, bytes)
    assert len(r1) == len(r2), "r1 and r2 should be the same length"
    xor_result: bytes = b"".join([bytes([x ^ y]) for (x, y) in
                                  zip(r1, r2)])
    return xor_result


def check_space(content: Iterable[int], threshold: float = 0.8) -> bool:
    assert isinstance(content, Iterable)
    result = list(map(count_chr, content))
    return sum(result) / len(result) > threshold


def main() -> None:
    total_r: List[bytes] = list(map(bytes.fromhex,
                                    [
                                        r1, r2, r3,
                                        r4, r5, r6,
                                        r7, r8, r9,
                                        r10, target
                                    ]))
    min_len: int = min(map(len, total_r))
    result_container: List[str] = [b'*'] * min_len
    # loop all encripted cyphers:
    for r in total_r:
        xored_rs: List[bytes] = [xor_2_bytes(r[:min_len], r2[:min_len]) for r2 in total_r]
        bytes_ref: List[Tuple] = list(zip(*xored_rs))
        for index, content in enumerate(bytes_ref):
            if check_space(content):
                result_container[index] = bytes([content[-1]]).swapcase()  # switch cases
    result = b''.join(result_container)
    print(result.replace(b'\x00', b' '))


if __name__ == '__main__':
    main()
