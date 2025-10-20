from passlib.hash import bcrypt_sha256, bcrypt
h = bcrypt_sha256.hash("123456789")
open("hash.txt","w",encoding="utf-8").write(h)
print("Len:", len(h))

bcrypt_sha256.identify(h)          # → True
bcrypt_sha256.verify("123456789", h)  # → True

h = "$bcrypt-sha256$v=2,t=2b,r=12$lvcYnT5nio6vcdHMSaxxve$1Am4P52pnz9jvavZ.ATBaWIALd6/Ihy"   # tal cual, sin espacios
print("prefix:", h[:25], "len:", len(h))
print("identify:", bcrypt_sha256.identify(h))
print("verify:", bcrypt_sha256.verify("123456789", h))