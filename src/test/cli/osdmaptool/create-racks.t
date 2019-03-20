  $ osdmaptool --create-from-conf om -c $TESTDIR/ceph.conf.withracks --with-default-pool
  osdmaptool: osdmap file 'om'
  osdmaptool: writing epoch 1 to om
  $ osdmaptool --export-crush oc om
  osdmaptool: osdmap file 'om'
  osdmaptool: exported crush map to oc
  $ crushtool --decompile oc
  # begin crush map
  tunable choose_local_tries 0
  tunable choose_local_fallback_tries 0
  tunable choose_total_tries 50
  tunable chooseleaf_descend_once 1
  tunable chooseleaf_vary_r 1
  tunable chooseleaf_stable 1
  tunable straw_calc_version 1
  tunable allowed_bucket_algs 54
  
  # devices
  device 1 osd.1
  device 2 osd.2
  device 3 osd.3
  device 4 osd.4
  device 5 osd.5
  device 6 osd.6
  device 7 osd.7
  device 8 osd.8
  device 9 osd.9
  device 10 osd.10
  device 11 osd.11
  device 12 osd.12
  device 13 osd.13
  device 14 osd.14
  device 15 osd.15
  device 16 osd.16
  device 17 osd.17
  device 18 osd.18
  device 19 osd.19
  device 20 osd.20
  device 21 osd.21
  device 22 osd.22
  device 23 osd.23
  device 24 osd.24
  device 25 osd.25
  device 26 osd.26
  device 27 osd.27
  device 28 osd.28
  device 29 osd.29
  device 30 osd.30
  device 31 osd.31
  device 32 osd.32
  device 33 osd.33
  device 34 osd.34
  device 35 osd.35
  device 36 osd.36
  device 37 osd.37
  device 38 osd.38
  device 39 osd.39
  device 40 osd.40
  device 41 osd.41
  device 42 osd.42
  device 43 osd.43
  device 44 osd.44
  device 45 osd.45
  device 46 osd.46
  device 47 osd.47
  device 48 osd.48
  device 49 osd.49
  device 50 osd.50
  device 51 osd.51
  device 52 osd.52
  device 53 osd.53
  device 54 osd.54
  device 55 osd.55
  device 56 osd.56
  device 57 osd.57
  device 58 osd.58
  device 59 osd.59
  device 60 osd.60
  device 61 osd.61
  device 62 osd.62
  device 63 osd.63
  device 64 osd.64
  device 65 osd.65
  device 66 osd.66
  device 67 osd.67
  device 68 osd.68
  device 69 osd.69
  device 70 osd.70
  device 71 osd.71
  device 72 osd.72
  device 73 osd.73
  device 74 osd.74
  device 75 osd.75
  device 76 osd.76
  device 77 osd.77
  device 78 osd.78
  device 79 osd.79
  device 80 osd.80
  device 81 osd.81
  device 82 osd.82
  device 83 osd.83
  device 84 osd.84
  device 85 osd.85
  device 86 osd.86
  device 87 osd.87
  device 88 osd.88
  device 89 osd.89
  device 90 osd.90
  device 91 osd.91
  device 92 osd.92
  device 93 osd.93
  device 94 osd.94
  device 95 osd.95
  device 96 osd.96
  device 97 osd.97
  device 98 osd.98
  device 99 osd.99
  device 100 osd.100
  device 101 osd.101
  device 102 osd.102
  device 103 osd.103
  device 104 osd.104
  device 105 osd.105
  device 106 osd.106
  device 107 osd.107
  device 108 osd.108
  device 109 osd.109
  device 110 osd.110
  device 111 osd.111
  device 112 osd.112
  device 113 osd.113
  device 114 osd.114
  device 115 osd.115
  device 116 osd.116
  device 117 osd.117
  device 118 osd.118
  device 119 osd.119
  device 120 osd.120
  device 121 osd.121
  device 122 osd.122
  device 123 osd.123
  device 124 osd.124
  device 125 osd.125
  device 126 osd.126
  device 127 osd.127
  device 128 osd.128
  device 129 osd.129
  device 130 osd.130
  device 131 osd.131
  device 132 osd.132
  device 133 osd.133
  device 134 osd.134
  device 135 osd.135
  device 136 osd.136
  device 137 osd.137
  device 138 osd.138
  device 139 osd.139
  device 140 osd.140
  device 141 osd.141
  device 142 osd.142
  device 143 osd.143
  device 144 osd.144
  device 145 osd.145
  device 146 osd.146
  device 147 osd.147
  device 148 osd.148
  device 149 osd.149
  device 150 osd.150
  device 151 osd.151
  device 152 osd.152
  device 153 osd.153
  device 154 osd.154
  device 155 osd.155
  device 156 osd.156
  device 157 osd.157
  device 158 osd.158
  device 159 osd.159
  device 160 osd.160
  device 161 osd.161
  device 162 osd.162
  device 163 osd.163
  device 164 osd.164
  device 165 osd.165
  device 166 osd.166
  device 167 osd.167
  device 168 osd.168
  device 169 osd.169
  device 170 osd.170
  device 171 osd.171
  device 172 osd.172
  device 173 osd.173
  device 174 osd.174
  device 175 osd.175
  device 176 osd.176
  device 177 osd.177
  device 178 osd.178
  device 179 osd.179
  device 180 osd.180
  device 181 osd.181
  device 182 osd.182
  device 183 osd.183
  device 184 osd.184
  device 185 osd.185
  device 186 osd.186
  device 187 osd.187
  device 188 osd.188
  device 189 osd.189
  device 190 osd.190
  device 191 osd.191
  device 192 osd.192
  device 193 osd.193
  device 194 osd.194
  device 195 osd.195
  device 196 osd.196
  device 197 osd.197
  device 198 osd.198
  device 199 osd.199
  device 200 osd.200
  device 201 osd.201
  device 202 osd.202
  device 203 osd.203
  device 204 osd.204
  device 205 osd.205
  device 206 osd.206
  device 207 osd.207
  device 208 osd.208
  device 209 osd.209
  device 210 osd.210
  device 211 osd.211
  device 212 osd.212
  device 213 osd.213
  device 214 osd.214
  device 215 osd.215
  device 216 osd.216
  device 217 osd.217
  device 218 osd.218
  device 219 osd.219
  device 220 osd.220
  device 221 osd.221
  device 222 osd.222
  device 223 osd.223
  device 224 osd.224
  device 225 osd.225
  device 226 osd.226
  device 227 osd.227
  device 228 osd.228
  device 229 osd.229
  device 230 osd.230
  device 231 osd.231
  device 232 osd.232
  device 233 osd.233
  device 234 osd.234
  device 235 osd.235
  device 236 osd.236
  device 237 osd.237
  device 238 osd.238
  
  # types
  type 0 osd
  type 1 host
  type 2 chassis
  type 3 rack
  type 4 row
  type 5 pdu
  type 6 pod
  type 7 room
  type 8 datacenter
  type 9 zone
  type 10 region
  type 11 root
  
  # buckets
  host cephstore5522 {
  \tid -2\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.1 weight 1.000 (esc)
  \titem osd.2 weight 1.000 (esc)
  \titem osd.3 weight 1.000 (esc)
  \titem osd.4 weight 1.000 (esc)
  \titem osd.5 weight 1.000 (esc)
  \titem osd.6 weight 1.000 (esc)
  \titem osd.7 weight 1.000 (esc)
  }
  host cephstore5523 {
  \tid -4\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.10 weight 1.000 (esc)
  \titem osd.11 weight 1.000 (esc)
  \titem osd.12 weight 1.000 (esc)
  \titem osd.13 weight 1.000 (esc)
  \titem osd.14 weight 1.000 (esc)
  \titem osd.8 weight 1.000 (esc)
  \titem osd.9 weight 1.000 (esc)
  }
  host cephstore6238 {
  \tid -8\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.113 weight 1.000 (esc)
  \titem osd.114 weight 1.000 (esc)
  \titem osd.115 weight 1.000 (esc)
  \titem osd.116 weight 1.000 (esc)
  \titem osd.117 weight 1.000 (esc)
  \titem osd.118 weight 1.000 (esc)
  \titem osd.119 weight 1.000 (esc)
  }
  host cephstore6240 {
  \tid -10\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.127 weight 1.000 (esc)
  \titem osd.128 weight 1.000 (esc)
  \titem osd.129 weight 1.000 (esc)
  \titem osd.130 weight 1.000 (esc)
  \titem osd.131 weight 1.000 (esc)
  \titem osd.132 weight 1.000 (esc)
  \titem osd.133 weight 1.000 (esc)
  }
  host cephstore6242 {
  \tid -12\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.141 weight 1.000 (esc)
  \titem osd.142 weight 1.000 (esc)
  \titem osd.143 weight 1.000 (esc)
  \titem osd.144 weight 1.000 (esc)
  \titem osd.145 weight 1.000 (esc)
  \titem osd.146 weight 1.000 (esc)
  \titem osd.147 weight 1.000 (esc)
  }
  host cephstore5524 {
  \tid -14\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.15 weight 1.000 (esc)
  \titem osd.16 weight 1.000 (esc)
  \titem osd.17 weight 1.000 (esc)
  \titem osd.18 weight 1.000 (esc)
  \titem osd.19 weight 1.000 (esc)
  \titem osd.20 weight 1.000 (esc)
  \titem osd.21 weight 1.000 (esc)
  }
  host cephstore6244 {
  \tid -15\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.155 weight 1.000 (esc)
  \titem osd.156 weight 1.000 (esc)
  \titem osd.157 weight 1.000 (esc)
  \titem osd.158 weight 1.000 (esc)
  \titem osd.159 weight 1.000 (esc)
  \titem osd.160 weight 1.000 (esc)
  \titem osd.161 weight 1.000 (esc)
  }
  host cephstore6246 {
  \tid -17\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.169 weight 1.000 (esc)
  \titem osd.170 weight 1.000 (esc)
  \titem osd.171 weight 1.000 (esc)
  \titem osd.172 weight 1.000 (esc)
  \titem osd.173 weight 1.000 (esc)
  \titem osd.174 weight 1.000 (esc)
  \titem osd.175 weight 1.000 (esc)
  }
  host cephstore6337 {
  \tid -19\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.183 weight 1.000 (esc)
  \titem osd.184 weight 1.000 (esc)
  \titem osd.185 weight 1.000 (esc)
  \titem osd.186 weight 1.000 (esc)
  \titem osd.187 weight 1.000 (esc)
  \titem osd.188 weight 1.000 (esc)
  \titem osd.189 weight 1.000 (esc)
  }
  host cephstore6341 {
  \tid -23\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.211 weight 1.000 (esc)
  \titem osd.212 weight 1.000 (esc)
  \titem osd.213 weight 1.000 (esc)
  \titem osd.214 weight 1.000 (esc)
  \titem osd.215 weight 1.000 (esc)
  \titem osd.216 weight 1.000 (esc)
  \titem osd.217 weight 1.000 (esc)
  }
  host cephstore6342 {
  \tid -24\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.218 weight 1.000 (esc)
  \titem osd.219 weight 1.000 (esc)
  \titem osd.220 weight 1.000 (esc)
  \titem osd.221 weight 1.000 (esc)
  \titem osd.222 weight 1.000 (esc)
  \titem osd.223 weight 1.000 (esc)
  \titem osd.224 weight 1.000 (esc)
  }
  host cephstore5525 {
  \tid -25\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.22 weight 1.000 (esc)
  \titem osd.23 weight 1.000 (esc)
  \titem osd.24 weight 1.000 (esc)
  \titem osd.25 weight 1.000 (esc)
  \titem osd.26 weight 1.000 (esc)
  \titem osd.27 weight 1.000 (esc)
  \titem osd.28 weight 1.000 (esc)
  }
  host cephstore6345 {
  \tid -27\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.232 weight 1.000 (esc)
  \titem osd.233 weight 1.000 (esc)
  \titem osd.234 weight 1.000 (esc)
  \titem osd.235 weight 1.000 (esc)
  \titem osd.236 weight 1.000 (esc)
  \titem osd.237 weight 1.000 (esc)
  \titem osd.238 weight 1.000 (esc)
  }
  host cephstore5526 {
  \tid -28\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.29 weight 1.000 (esc)
  \titem osd.30 weight 1.000 (esc)
  \titem osd.31 weight 1.000 (esc)
  \titem osd.32 weight 1.000 (esc)
  \titem osd.33 weight 1.000 (esc)
  \titem osd.34 weight 1.000 (esc)
  \titem osd.35 weight 1.000 (esc)
  }
  host cephstore5527 {
  \tid -29\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.36 weight 1.000 (esc)
  \titem osd.37 weight 1.000 (esc)
  \titem osd.38 weight 1.000 (esc)
  \titem osd.39 weight 1.000 (esc)
  \titem osd.40 weight 1.000 (esc)
  \titem osd.41 weight 1.000 (esc)
  \titem osd.42 weight 1.000 (esc)
  }
  host cephstore5529 {
  \tid -30\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.43 weight 1.000 (esc)
  \titem osd.44 weight 1.000 (esc)
  \titem osd.45 weight 1.000 (esc)
  \titem osd.46 weight 1.000 (esc)
  \titem osd.47 weight 1.000 (esc)
  \titem osd.48 weight 1.000 (esc)
  \titem osd.49 weight 1.000 (esc)
  }
  host cephstore5530 {
  \tid -31\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.50 weight 1.000 (esc)
  \titem osd.51 weight 1.000 (esc)
  \titem osd.52 weight 1.000 (esc)
  \titem osd.53 weight 1.000 (esc)
  \titem osd.54 weight 1.000 (esc)
  \titem osd.55 weight 1.000 (esc)
  \titem osd.56 weight 1.000 (esc)
  }
  rack irv-n2 {
  \tid -3\t\t# do not change unnecessarily (esc)
  \t# weight 119.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem cephstore5522 weight 7.000 (esc)
  \titem cephstore5523 weight 7.000 (esc)
  \titem cephstore6238 weight 7.000 (esc)
  \titem cephstore6240 weight 7.000 (esc)
  \titem cephstore6242 weight 7.000 (esc)
  \titem cephstore5524 weight 7.000 (esc)
  \titem cephstore6244 weight 7.000 (esc)
  \titem cephstore6246 weight 7.000 (esc)
  \titem cephstore6337 weight 7.000 (esc)
  \titem cephstore6341 weight 7.000 (esc)
  \titem cephstore6342 weight 7.000 (esc)
  \titem cephstore5525 weight 7.000 (esc)
  \titem cephstore6345 weight 7.000 (esc)
  \titem cephstore5526 weight 7.000 (esc)
  \titem cephstore5527 weight 7.000 (esc)
  \titem cephstore5529 weight 7.000 (esc)
  \titem cephstore5530 weight 7.000 (esc)
  }
  host cephstore6236 {
  \tid -5\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.100 weight 1.000 (esc)
  \titem osd.101 weight 1.000 (esc)
  \titem osd.102 weight 1.000 (esc)
  \titem osd.103 weight 1.000 (esc)
  \titem osd.104 weight 1.000 (esc)
  \titem osd.105 weight 1.000 (esc)
  \titem osd.99 weight 1.000 (esc)
  }
  host cephstore6237 {
  \tid -7\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.106 weight 1.000 (esc)
  \titem osd.107 weight 1.000 (esc)
  \titem osd.108 weight 1.000 (esc)
  \titem osd.109 weight 1.000 (esc)
  \titem osd.110 weight 1.000 (esc)
  \titem osd.111 weight 1.000 (esc)
  \titem osd.112 weight 1.000 (esc)
  }
  host cephstore6239 {
  \tid -9\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.120 weight 1.000 (esc)
  \titem osd.121 weight 1.000 (esc)
  \titem osd.122 weight 1.000 (esc)
  \titem osd.123 weight 1.000 (esc)
  \titem osd.124 weight 1.000 (esc)
  \titem osd.125 weight 1.000 (esc)
  \titem osd.126 weight 1.000 (esc)
  }
  host cephstore6241 {
  \tid -11\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.134 weight 1.000 (esc)
  \titem osd.135 weight 1.000 (esc)
  \titem osd.136 weight 1.000 (esc)
  \titem osd.137 weight 1.000 (esc)
  \titem osd.138 weight 1.000 (esc)
  \titem osd.139 weight 1.000 (esc)
  \titem osd.140 weight 1.000 (esc)
  }
  host cephstore6243 {
  \tid -13\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.148 weight 1.000 (esc)
  \titem osd.149 weight 1.000 (esc)
  \titem osd.150 weight 1.000 (esc)
  \titem osd.151 weight 1.000 (esc)
  \titem osd.152 weight 1.000 (esc)
  \titem osd.153 weight 1.000 (esc)
  \titem osd.154 weight 1.000 (esc)
  }
  host cephstore6245 {
  \tid -16\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.162 weight 1.000 (esc)
  \titem osd.163 weight 1.000 (esc)
  \titem osd.164 weight 1.000 (esc)
  \titem osd.165 weight 1.000 (esc)
  \titem osd.166 weight 1.000 (esc)
  \titem osd.167 weight 1.000 (esc)
  \titem osd.168 weight 1.000 (esc)
  }
  host cephstore6336 {
  \tid -18\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.176 weight 1.000 (esc)
  \titem osd.177 weight 1.000 (esc)
  \titem osd.178 weight 1.000 (esc)
  \titem osd.179 weight 1.000 (esc)
  \titem osd.180 weight 1.000 (esc)
  \titem osd.181 weight 1.000 (esc)
  \titem osd.182 weight 1.000 (esc)
  }
  host cephstore6338 {
  \tid -20\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.190 weight 1.000 (esc)
  \titem osd.191 weight 1.000 (esc)
  \titem osd.192 weight 1.000 (esc)
  \titem osd.193 weight 1.000 (esc)
  \titem osd.194 weight 1.000 (esc)
  \titem osd.195 weight 1.000 (esc)
  \titem osd.196 weight 1.000 (esc)
  }
  host cephstore6339 {
  \tid -21\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.197 weight 1.000 (esc)
  \titem osd.198 weight 1.000 (esc)
  \titem osd.199 weight 1.000 (esc)
  \titem osd.200 weight 1.000 (esc)
  \titem osd.201 weight 1.000 (esc)
  \titem osd.202 weight 1.000 (esc)
  \titem osd.203 weight 1.000 (esc)
  }
  host cephstore6340 {
  \tid -22\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.204 weight 1.000 (esc)
  \titem osd.205 weight 1.000 (esc)
  \titem osd.206 weight 1.000 (esc)
  \titem osd.207 weight 1.000 (esc)
  \titem osd.208 weight 1.000 (esc)
  \titem osd.209 weight 1.000 (esc)
  \titem osd.210 weight 1.000 (esc)
  }
  host cephstore6343 {
  \tid -26\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.225 weight 1.000 (esc)
  \titem osd.226 weight 1.000 (esc)
  \titem osd.227 weight 1.000 (esc)
  \titem osd.228 weight 1.000 (esc)
  \titem osd.229 weight 1.000 (esc)
  \titem osd.230 weight 1.000 (esc)
  \titem osd.231 weight 1.000 (esc)
  }
  host cephstore6230 {
  \tid -32\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.57 weight 1.000 (esc)
  \titem osd.58 weight 1.000 (esc)
  \titem osd.59 weight 1.000 (esc)
  \titem osd.60 weight 1.000 (esc)
  \titem osd.61 weight 1.000 (esc)
  \titem osd.62 weight 1.000 (esc)
  \titem osd.63 weight 1.000 (esc)
  }
  host cephstore6231 {
  \tid -33\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.64 weight 1.000 (esc)
  \titem osd.65 weight 1.000 (esc)
  \titem osd.66 weight 1.000 (esc)
  \titem osd.67 weight 1.000 (esc)
  \titem osd.68 weight 1.000 (esc)
  \titem osd.69 weight 1.000 (esc)
  \titem osd.70 weight 1.000 (esc)
  }
  host cephstore6232 {
  \tid -34\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.71 weight 1.000 (esc)
  \titem osd.72 weight 1.000 (esc)
  \titem osd.73 weight 1.000 (esc)
  \titem osd.74 weight 1.000 (esc)
  \titem osd.75 weight 1.000 (esc)
  \titem osd.76 weight 1.000 (esc)
  \titem osd.77 weight 1.000 (esc)
  }
  host cephstore6233 {
  \tid -35\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.78 weight 1.000 (esc)
  \titem osd.79 weight 1.000 (esc)
  \titem osd.80 weight 1.000 (esc)
  \titem osd.81 weight 1.000 (esc)
  \titem osd.82 weight 1.000 (esc)
  \titem osd.83 weight 1.000 (esc)
  \titem osd.84 weight 1.000 (esc)
  }
  host cephstore6234 {
  \tid -36\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.85 weight 1.000 (esc)
  \titem osd.86 weight 1.000 (esc)
  \titem osd.87 weight 1.000 (esc)
  \titem osd.88 weight 1.000 (esc)
  \titem osd.89 weight 1.000 (esc)
  \titem osd.90 weight 1.000 (esc)
  \titem osd.91 weight 1.000 (esc)
  }
  host cephstore6235 {
  \tid -37\t\t# do not change unnecessarily (esc)
  \t# weight 7.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem osd.92 weight 1.000 (esc)
  \titem osd.93 weight 1.000 (esc)
  \titem osd.94 weight 1.000 (esc)
  \titem osd.95 weight 1.000 (esc)
  \titem osd.96 weight 1.000 (esc)
  \titem osd.97 weight 1.000 (esc)
  \titem osd.98 weight 1.000 (esc)
  }
  rack irv-n1 {
  \tid -6\t\t# do not change unnecessarily (esc)
  \t# weight 119.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem cephstore6236 weight 7.000 (esc)
  \titem cephstore6237 weight 7.000 (esc)
  \titem cephstore6239 weight 7.000 (esc)
  \titem cephstore6241 weight 7.000 (esc)
  \titem cephstore6243 weight 7.000 (esc)
  \titem cephstore6245 weight 7.000 (esc)
  \titem cephstore6336 weight 7.000 (esc)
  \titem cephstore6338 weight 7.000 (esc)
  \titem cephstore6339 weight 7.000 (esc)
  \titem cephstore6340 weight 7.000 (esc)
  \titem cephstore6343 weight 7.000 (esc)
  \titem cephstore6230 weight 7.000 (esc)
  \titem cephstore6231 weight 7.000 (esc)
  \titem cephstore6232 weight 7.000 (esc)
  \titem cephstore6233 weight 7.000 (esc)
  \titem cephstore6234 weight 7.000 (esc)
  \titem cephstore6235 weight 7.000 (esc)
  }
  root default {
  \tid -1\t\t# do not change unnecessarily (esc)
  \t# weight 238.000 (esc)
  \talg straw2 (esc)
  \thash 0\t# rjenkins1 (esc)
  \titem irv-n2 weight 119.000 (esc)
  \titem irv-n1 weight 119.000 (esc)
  }
  
  # rules
  rule replicated_rule {
  \tid 0 (esc)
  \ttype replicated (esc)
  \tmin_size 1 (esc)
  \tmax_size 10 (esc)
  \tstep take default (esc)
  \tstep chooseleaf firstn 0 type host (esc)
  \tstep emit (esc)
  }
  
  # end crush map
  $ rm oc
  $ osdmaptool --test-map-pg 0.0 om
  osdmaptool: osdmap file 'om'
   parsed '0.0' -> 0.0
  0.0 raw ([], p-1) up ([], p-1) acting ([], p-1)
  $ osdmaptool --print om
  osdmaptool: osdmap file 'om'
  epoch 1
  fsid [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} (re)
  created \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  modified \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ (re)
  flags 
  crush_version 1
  full_ratio 0
  backfillfull_ratio 0
  nearfull_ratio 0
  min_compat_client jewel
  
  pool 1 'rbd' replicated size 3 min_size 2 crush_rule 0 object_hash rjenkins pg_num 15296 pgp_num 15296 autoscale_mode warn last_change 0 flags hashpspool stripe_width 0 application rbd
  
  max_osd 239
  

  $ osdmaptool --clobber --create-from-conf --with-default-pool om -c $TESTDIR/ceph.conf.withracks
  osdmaptool: osdmap file 'om'
  osdmaptool: writing epoch 1 to om
  $ osdmaptool --print om | grep 'pool 1'
  osdmaptool: osdmap file 'om'
  pool 1 'rbd' replicated size 3 min_size 2 crush_rule 0 object_hash rjenkins pg_num 15296 pgp_num 15296 autoscale_mode warn last_change 0 flags hashpspool stripe_width 0 application rbd
  $ rm -f om
