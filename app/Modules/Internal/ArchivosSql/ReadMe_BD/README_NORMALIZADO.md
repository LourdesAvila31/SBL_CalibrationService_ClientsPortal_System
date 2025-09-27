# Datos normalizados

Este documento conserva la información histórica que antes se distribuía en `normalizado.sql`. Se presentan los datos de referencia para catálogos y tablas relacionadas en formato legible.

## Departamentos (`departamentos`)

```text
id  nombre
1   Almacén
2   Ambiental
3   Control de Calidad
4   Desarrollo Analítico
5   Desarrollo Farmacéutico
6   Fabricación
7   Gestión de la Calidad
8   Logística
9   Mantenimiento
10  Microbiología
11  Validación
12  NA
```

## Catálogo de instrumentos (`catalogo_instrumentos`)

```text
id  nombre
1   Aerotest
2   Agitador magnético
3   Agitador propela
4   Agitador Vórtex
5   Amperímetro
6   Anemómetro
7   Balanza analítica
8   Balanza electrónica
9   Balanza semi-microanalítica
10  Balómetro
11  Baño de agua
12  Báscula digital
13  Bomba peristáltica
14  Calefactor
15  Contador de partículas
16  Controlador de temperatura de formado
17  Controlador de temperatura de sellado
18  Criotermostato de recirculación
19  Cronómetro
20  Densímetro
21  Durómetro
22  Electrodo
23  Filtro HEPA 24*26*5 7/8
24  Filtro HEPA 510 x 510 x 305 MM
25  Filtro HEPA 24" x 24" x 11.5" inyección
26  Filtro HEPA 24" x 24" x 11.5" extracción
27  Fusiómetro
28  Humidostato
29  Lámpara UV
30  Lápiz de humo
31  Luxómetro
32  Manómetro
33  Manómetro de presión
34  Manómetro de vacío
35  Manómetro diferencial
36  Manómetro Diferencial Digital
37  Marco de masas
38  Masa
39  Masa 1 mg
40  Medidor multiparamétrico
41  Micrómetro
42  Micropipeta
43  Microscopio
44  Muestreador de aire
45  Multímetro
46  Multiparamétrico
47  Nivel
48  Parrilla con agitador
49  Parrilla eléctrica
50  Pesa 5Kg
51  Pesa 10Kg
52  Pesa 25Kg
53  Pesa 50Kg
54  Pistola de recubrimiento
55  Punzón 6 mm
56  Punzón 8 mm
57  Punzón 10 mm
58  Punzón 11.1 mm x 5.5 mm
59  Purificador de agua
60  Registrador de temperatura
61  Registrador de temperatura y humedad
62  Inductor de calor
63  Sensor de presión
64  Sensor de punto de rocío
65  Sensor de temperatura
66  Sistema de filtros de agua
67  Sistema de filtros de UMAs
68  Sistema de filtros
69  Sistema de filtros de aire comprimido
70  Sonicador
71  Tacómetro
72  Tambor
73  Temporizador
74  Termobalanza
75  Termohigrómetro
76  Termómetro
77  Termómetro digital
78  Termómetro IR
79  Termopar
80  Termostato programable
81  Transmisor de temperatura y humedad
82  Vernier
83  Viscosímetro
```

## Marcas (`marcas`)

```text
id  nombre
1   Dräger
2   Corning
3   VWR
4   Heidolph
5   ND
6   Thomas Scientific
7   Scientific Industries
8   Fluke
9   Extech Instruments / Extech Instruments
10  CAS
11  Ohaus
12  AND
13  ADAM
14  Alnor TSI Incorporated / Alnor
15  Watson Marlow
16  NA
17  Lighthouse
18  OMRON
19  Julabo
20  Control Company
21  Fisher Scientific
22  General Tools
23  Hanhart
24  Pharma Alliance
25  Dillon
26  ELECTROLAB
27  HARTEK
28  HANNA Instruments
29  Mettler Toledo
30  OAKTON
31  Thermo Eutech
32  VECOFLOW
33  Freudenberg
34  Greenfilt
35  Thermo Scientific
36  Honeywell
37  Analytik Jena US
38  Entela
39  Chimney Balloon
40  HIOKI
41  USG
42  C.A. Norgreen Co.
43  Ashcroft
44  MC
45  Winters
46  SMC / SMC Pneumatics
47  TRU-FLATE
48  Tube & Socket
49  Wika
50  PRO-BLOCK
51  Puroflo
52  Dwyer Instruments / Dwyer
53  SENSOCON
54  Dewit / DEWIT
55  Aquex
56  ENERPAC
57  MASS
58  SNS
59  Power Team
60  Instrutek
61  Manometer
62  Huayi
63  TROEMNER
64  SUMIMET
65  Mitutoyo
66  Eppendorf
67  Brand
68  LABOMED
69  TRIO.BAS
70  Truper
71  HACH / HATCH
72  SPI-TRONIC
73  DLAB
74  Rice Lake
75  Binks
76  ELGA Purelab
77  Graphtec
78  Omega
79  Amprobe
80  CEM
81  Ampere
82  Lascar Electronics
83  JORESTECH
84  CS Instruments
85  Sensewell
86  Novus
87  ASTROCEL I
88  Festo
89  Tianyu
90  JAJA
91  Branson
92  Autonics
93  Sper Scientific
94  HUATO
95  Alla France
96  STB-22
97  Nachtman
98  Fungilab
99  Neiko
100  Carrier
```

## Modelos (`modelos`)

```text
id   nombre                                  marca_id
1    Marca                                   NULL
2    Alpha                                   NULL
3    PC-410                                  NULL
4    97042-626                               NULL
5    986920                                  NULL
6    PC-410D                                 NULL
7    RZR2020                                 NULL
8    ND                                      NULL
9    Digital Genie2 120V                     NULL
10   LSE                                     NULL
11   Digital Vortex-Genie 2                  NULL
12   323                                     NULL
13   325                                     NULL
14   Traceable Hot Wire                      NULL
15   Field Master                            NULL
16   XB420HW                                 NULL
17   Adventurer PRO                          NULL
18   AX124                                   NULL
19   PA224                                   NULL
20   PX224/E                                 NULL
21   HR-120                                  NULL
22   ABL 225                                 NULL
23   AX223                                   NULL
24   Scout Pro SP601                         NULL
25   SPX222                                  NULL
26   GH-202                                  NULL
27   PX225D                                  NULL
28   Loflo 6200D                             NULL
29   6200                                    NULL
30   142665-PC                               NULL
31   5L                                      NULL
32   EC-II                                   NULL
33   DBB Jr.                                 NULL
34   RC301P30                                NULL
35   HD                                      NULL
36   Ranger 3000                             NULL
37   RC31P30                                 NULL
38   XE-6000                                 NULL
39   DBB-Jr                                  NULL
40   HDI                                     NULL
41   HDI/SPS 4050                            NULL
42   HDL-SPS4050                             NULL
43   HDI/SPS4050                             NULL
44   HDI/SPS4051                             NULL
45   1205/DV                                 NULL
46   NA                                      NULL
47   SOLAIR 3100+                            NULL
48   S3100                                   NULL
49   ESCWL-RITC                              NULL
50   CORIO CD-201F                           NULL
51   1235D46                                 NULL
52   365510                                  NULL
53   5017                                    NULL
54   5008                                    NULL
55   62344-912                               NULL
56   8788V77                                 NULL
57   1043                                    NULL
58   SW888L                                  NULL
59   Fingertip                               NULL
60   Compact 2                               NULL
61   5CVT8                                   NULL
62   TD-12                                   NULL
63   MHT-100                                 NULL
64   ETB-2PRL                                NULL
65   PHT-500P                                NULL
66   Halo HI11102                            NULL
67   perfectION comb F- Lemo                 NULL
68   DM143-SC                                NULL
69   HI76330                                 NULL
70   HI 1053P                                NULL
71   HI 1131P                                NULL
72   35805-04                                NULL
73   35411-00                                NULL
74   3002742                                 NULL
75   Electrode WD-35805-04                   NULL
76   DM141-SC                                NULL
77   DGi 113-SC                              NULL
78   DGi 111-SC                              NULL
79   HE9T16SAMA                              NULL
80   510 X 510 X 305 MM                      NULL
81   1101D                                   NULL
82   H6062A1000                              NULL
83   UVP UVGL-58                             NULL
84   UVGL-58                                 NULL
85   Smoke pencil                            NULL
86   HI 97500                                NULL
87   FT3424                                  NULL
88   01 000127                               NULL
89   0-100 Psi                               NULL
90   Pressure Gage                           NULL
91   1010                                    NULL
92   304SS Case, 316SS                       NULL
93   4CFE4A                                  NULL
94   Minihelic II                            NULL
95   EN837.1                                 NULL
96   HT-K16                                  NULL
97   PFQ-LF                                  NULL
98   BG                                      NULL
99   150 PSI                                 NULL
100  G2535L                                  NULL
101  4CFL6A                                  NULL
102  MPG-28                                  NULL
103  63-GFB-B-L-13-C-RANGE-KG/CM2-GR-UF      NULL
104  9055                                    NULL
105  KAOGEER                                 NULL
106  2000CB                                  NULL
107  B8336                                   NULL
108  927                                     NULL
109  4CFD8A                                  NULL
110  255/63/VACIO                            NULL
111  2300-0                                  NULL
112  2302                                    NULL
113  2002-C                                  NULL
114  2003                                    NULL
115  2001                                    NULL
116  2001-C                                  NULL
117  S2000-1000MM                            NULL
118  Minihelic II 2-5005                     NULL
119  2300                                    NULL
120  2300-1                                  NULL
121  S2300-200MM                             NULL
122  DHC-202                                 NULL
123  STO-A-WEIGH                             NULL
124  ASTM 100G-1MG CL4                       NULL
125  OIML R 111                              NULL
126  1g                                      NULL
127  50g                                     NULL
128  100g                                    NULL
129  200                                     NULL
130  10g                                     NULL
131  11124401PM1                             NULL
132  PCS Testr35                             NULL
133  293-344-30                              NULL
134  MDC-1"SXF                               NULL
135  VE100                                   NULL
136  VE1000                                  NULL
137  VE5000                                  NULL
138  Finnpipette F1                          NULL
139  0.5-5 mL                                NULL
140  100-1000 μL                             NULL
141  Research plus 100-1000 μL               NULL
142  VWR EHP1000 1CH                         NULL
143  Transferpette                           NULL
144  Finnpipette F2 0.5-5 mL                 NULL
145  Finnpipette F2 100-1000 μL              NULL
146  Finnpipette F2 10-100 μL                NULL
147  82026-622                               NULL
148  CxL                                     NULL
149  MONO                                    NULL
150  GAS                                     NULL
151  MUT-039                                 NULL
152  Pocket Pro+Multi2                       NULL
153  POCKET PRO TESTER                       NULL
154  PRO 3600                                NULL
155  PC-420D                                 NULL
156  12365-452                               NULL
157  10x10                                   NULL
158  984VW0CHPUSP                            NULL
159  Clase F                                 NULL
160  BBR HVLP                                NULL
161  Ultra ANMK2                             NULL
162  GL220                                   NULL
163  OM-EL-USB-1-PRO-A                       NULL
164  TB300                                   NULL
165  DT-171                                  NULL
166  EL-USB-2                                NULL
167  IND-100HA                               NULL
168  FA-510                                  NULL
169  RTD,PT-100                              NULL
170  RHT-DM                                  NULL
171  HET5SAML1C                              NULL
172  HET9SAML1C                              NULL
173  Tipo C                                  NULL
174  MS9-LWS-G-U-V                           NULL
175  MS6-LF-1/2-CRM                          NULL
176  MS6-LFM-1/2-BUV-HF-DA                   NULL
177  MS6-LFM-1/2-AUV-HF-DA                   NULL
178  MS6-LFM-1/2-AR-HF-DA:*W                 NULL
179  AW-20                                   NULL
180  AM250C                                  NULL
181  AMD250C                                 NULL
182  AFR 2000                                NULL
183  AL 2000                                 NULL
184  MS6-LF-1/2-CRV                          NULL
185  MS6-LFM-1/2-BRM-DA                      NULL
186  MS6-LFM-1/2-ARV-DA                      NULL
187  575HTA                                  NULL
188  2510R-MTH                               NULL
189  M8800                                   NULL
190  CPX8800H                                NULL
191  TACH-10                                 NULL
192  4060                                    NULL
193  LE4S                                    NULL
194  HB43                                    NULL
195  445702                                  NULL
196  445703                                  NULL
197  445713                                  NULL
198  800027                                  NULL
199  A2000-EX                                NULL
200  445701                                  NULL
201  4352                                    NULL
202  Traceable                               NULL
203  55000P110-qp                            NULL
204  76mm                                    NULL
205  307055                                  NULL
206  51 II                                   NULL
207  62 max                                  NULL
208  566                                     NULL
209  568                                     NULL
210  62 MAX                                  NULL
211  K                                       NULL
212  80PK-22                                 NULL
213  35613-13                                NULL
214  93X052911                               NULL
215  HI7662-T                                NULL
216  TH800                                   NULL
217  TH4110U2005                             NULL
218  TH5110D1006                             NULL
219  TH1110DV1009                            NULL
220  TH6220U2000                             NULL
221  RHT-DM-485-LCD                          NULL
222  CD-6" ASX                               NULL
223  01408A                                  NULL
224  No. 147                                 NULL
225  6 In                                    NULL
226  E008                                    NULL
227  CD-8"ASX                                NULL
228  SMART series                            NULL
229  KJR-12B/DP(T)-E                        NULL
230  KJR-29B1/BK-E                         NULL
231  TH2320U4006                        NULL
```
