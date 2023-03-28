# UNDP Job Postings

This Repo contains all currently (June 2022) available UNDP Job Postings ranging from 2004 to 2022. The Repository entails the script for scraping and storing the raw job postings from UNDP's server as well as a script for preprocessing the data. Analysis and Dashboarding is planned.

Cleaning scripts are combined with amazing open sourc work from https://github.com/alinacherkas
# Data after processing


```python
import pandas as pd
```


```python
df = pd.read_parquet("data/undp_jobs_processed.parquet")
```


```python
df.head(20)
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>job_id</th>
      <th>content</th>
      <th>title</th>
      <th>country</th>
      <th>region</th>
      <th>year</th>
      <th>type_of_contract</th>
      <th>post_level</th>
      <th>languages_required</th>
      <th>staff_category</th>
      <th>background</th>
      <th>duties_responsibilities</th>
      <th>competencies</th>
      <th>skills_experiences</th>
      <th>language</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>JOB20338108876596148de9fba9a926ad06</td>
      <td>['hiv and sti clinical consultant (ic)', 'loca...</td>
      <td>hiv and sti clinical consultant (ic)</td>
      <td>FJ</td>
      <td>Australia and Oceania</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>International Consultant</td>
      <td>English</td>
      <td>International Consultant</td>
      <td>the united nations development programme (undp...</td>
      <td>project description and consultancy rationale ...</td>
      <td>strong interpersonal and communication skills;...</td>
      <td>educational qualifications : minimum master s ...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>1</th>
      <td>JOB579f6f45d13396afc130172715c7bba3</td>
      <td>['internship- pacific digital economy programm...</td>
      <td>internship- pacific digital economy programme ...</td>
      <td>SB</td>
      <td>Australia and Oceania</td>
      <td>2022</td>
      <td>Internship</td>
      <td>Intern</td>
      <td>English</td>
      <td>Intern</td>
      <td>the united nations capital development fund (u...</td>
      <td>under the guidance and supervision of unc df s...</td>
      <td>uncdf/undp core competencies : communication d...</td>
      <td>education : candidate must be enrolled in a de...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>2</th>
      <td>JOB2b8aa5ccf8370e1acb7ba55cdc1e27d9</td>
      <td>['consultant international spécialisé dans le ...</td>
      <td>consultant international spécialisé dans le co...</td>
      <td>DJ</td>
      <td>Sub-Saharian Africa</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>International Consultant</td>
      <td>French</td>
      <td>International Consultant</td>
      <td>a vis de r ecru te ment d un consultant in div...</td>
      <td>2. description du pro jet le pro jet d ap pui ...</td>
      <td>4. liv rables attenduslivrables/résultatsdurée...</td>
      <td>7. qualifications et experiences requisesi. qu...</td>
      <td>fr</td>
    </tr>
    <tr>
      <th>3</th>
      <td>JOB6c058f6289a3634356df9788d72fb6a8</td>
      <td>['consultant national pour l’élaboration du pl...</td>
      <td>consultant national pour l’élaboration du plan...</td>
      <td>DJ</td>
      <td>Sub-Saharian Africa</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>National Consultant</td>
      <td>French</td>
      <td>National Consultant</td>
      <td>a vis de r ecru te ment d un consultant in div...</td>
      <td>3. object if s l object if principal de la mis...</td>
      <td>9. qualification le ou la consultant do it pos...</td>
      <td>10. term es de pai e men tle consultant sera p...</td>
      <td>fr</td>
    </tr>
    <tr>
      <th>4</th>
      <td>JOB72bdf033a137fb5a2dd18ad4b6941cdd</td>
      <td>['individual consultant - national project off...</td>
      <td>individual consultant - national project officer</td>
      <td>SA</td>
      <td>Western Asia</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>National Consultant</td>
      <td>English, Arabic</td>
      <td>National Consultant</td>
      <td>post title : national project officer starting...</td>
      <td>scope of work :       ensure effective and eff...</td>
      <td>competencies : corporate competencies : demons...</td>
      <td>required skills and experience :        educat...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>5</th>
      <td>JOBaf1f60615b47a0e25d635679003042ee</td>
      <td>['ic-023-22 national ecotourism site design ex...</td>
      <td>ic-023-22 national ecotourism site design expe...</td>
      <td>Home-based</td>
      <td>Home-based</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>National Consultant</td>
      <td>English, Arabic</td>
      <td>National Consultant</td>
      <td>undp iraq seeks to implement a 3-year “climate...</td>
      <td>the national ecotourism site design expert wil...</td>
      <td>corporate competencies : demonstrates integrit...</td>
      <td>qualifications, skills, and professional exper...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>6</th>
      <td>JOB089a82f6bf0f937885ea7a1189429ff5</td>
      <td>['development of materials and facilitation of...</td>
      <td>development of materials and facilitation of t...</td>
      <td>MZ</td>
      <td>Sub-Saharian Africa</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>National Consultant</td>
      <td>English</td>
      <td>National Consultant</td>
      <td>the sd g "localization" and the 2030 agenda re...</td>
      <td>training course on sd g localization methodolo...</td>
      <td>corporate competences : demonstrates integrity...</td>
      <td>experience and qualifications academic qualifi...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>7</th>
      <td>JOB80883842e87f7df110ac4239ed0abb25</td>
      <td>['international consultant – ppg gender specia...</td>
      <td>international consultant – ppg gender speciali...</td>
      <td>FJ</td>
      <td>Australia and Oceania</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>International Consultant</td>
      <td>Unspecified</td>
      <td>International Consultant</td>
      <td>following from the success of the tonga r 2 r ...</td>
      <td>scope of work the international gender special...</td>
      <td>strong interpersonal and communication skills;...</td>
      <td>educational qualifications : master s degree o...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>8</th>
      <td>JOB2f3528147d720b140c9bbc7a8a7f2d5d</td>
      <td>['international specialist(s) on climate chang...</td>
      <td>international specialist(s) on climate change ...</td>
      <td>Home-based</td>
      <td>Home-based</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>International Consultant</td>
      <td>English</td>
      <td>International Consultant</td>
      <td>undp cambodia country office works in partners...</td>
      <td>the main role of the senior international spec...</td>
      <td>functional competencies : strong reporting and...</td>
      <td>education :  master s degree in climate change...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>9</th>
      <td>JOB97bc4f050ce776d284ae23045c15c043</td>
      <td>['operations and programme support coordinator...</td>
      <td>operations and programme support coordinator</td>
      <td>JO</td>
      <td>Western Asia</td>
      <td>2022</td>
      <td>FTA Local</td>
      <td>NO-A</td>
      <td>English, Arabic</td>
      <td>National Professional</td>
      <td>within the framework of the regional programme...</td>
      <td>under the direct supervision of the uno dc cou...</td>
      <td>corporate competencies : demonstrates commitme...</td>
      <td>education : bachelor s degree in business admi...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>10</th>
      <td>JOB8bdd536e33093ed8fae47bfd74485189</td>
      <td>['consultant national pour l’élaboration de la...</td>
      <td>consultant national pour l’élaboration de la s...</td>
      <td>DJ</td>
      <td>Sub-Saharian Africa</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>National Consultant</td>
      <td>Unspecified</td>
      <td>National Consultant</td>
      <td>a vis de r ecru te ment d un consultant in div...</td>
      <td>2.justificationl’un des object if s de la visi...</td>
      <td>9. qualification le ou la consultant do it pos...</td>
      <td>term es de pai e men tle consultant sera payé ...</td>
      <td>fr</td>
    </tr>
    <tr>
      <th>11</th>
      <td>JOB3e512e141d58b9fda8c756d0b7ed362b</td>
      <td>['programme associate - ending violence agains...</td>
      <td>programme associate - ending violence against ...</td>
      <td>VU</td>
      <td>Australia and Oceania</td>
      <td>2022</td>
      <td>Service Contract</td>
      <td>SB/SC/GS-3</td>
      <td>English</td>
      <td>General Service</td>
      <td>un women, grounded in the vision of equality e...</td>
      <td>provide advanced administrative and logistical...</td>
      <td>key performance indicators : timely and accura...</td>
      <td>education : completion of secondary education ...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>12</th>
      <td>JOB5ebb99b2e183f711995b8ce115fb20c4</td>
      <td>['lead consultant on humanitarian assistance a...</td>
      <td>lead consultant on humanitarian assistance and...</td>
      <td>Home-based</td>
      <td>Home-based</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>International Consultant</td>
      <td>English</td>
      <td>International Consultant</td>
      <td>the revised national development strategy (202...</td>
      <td>undp and w fp have commissioned this study to ...</td>
      <td>core competencies : demonstrating/safeguarding...</td>
      <td>education a phd or a master s degree in the fi...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>14</th>
      <td>JOB919010545cb6dcb0a514d33f9cd983b4</td>
      <td>['call for volunteers: sustainable finance exp...</td>
      <td>call for volunteers :  sustainable finance exp...</td>
      <td>Home-based</td>
      <td>Home-based</td>
      <td>2022</td>
      <td>UNV</td>
      <td>UNV</td>
      <td>English</td>
      <td>UNV</td>
      <td>un women, grounded in the vision of equality e...</td>
      <td>key functions : review the assessment of about...</td>
      <td>integrity : demonstrate consistency in upholdi...</td>
      <td>education : advanced university degree in busi...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>15</th>
      <td>JOBde40cafc4ca944d09912920952fa4e22</td>
      <td>['english/dutch translation and interpretation...</td>
      <td>english/dutch translation and interpretation</td>
      <td>Home-based</td>
      <td>Home-based</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>International Consultant</td>
      <td>English</td>
      <td>International Consultant</td>
      <td>un women, grounded in the vision of equality e...</td>
      <td>purpose of the consultancy as part of implemen...</td>
      <td>core values : respect for diversity;integrity;...</td>
      <td>education degree or certification or equivalen...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>16</th>
      <td>JOB1e2cea0a0a4693a591faf7990b896202</td>
      <td>['onumujeres/ecu/ps/22-008 asistencia técnica ...</td>
      <td>onumujeres/ecu/ps/22-008 asistencia técnica pa...</td>
      <td>EC</td>
      <td>Latin America and the Carribean</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>National Consultant</td>
      <td>Spanish</td>
      <td>National Consultant</td>
      <td>1.contexto organ i zac ional la e ntida d de l...</td>
      <td>4.alcance de los obj et ivo sse esp era que el...</td>
      <td>8.indicadores de rendimientoproductos/ entre g...</td>
      <td>11.procedimiento de s ele cci n y re quis it o...</td>
      <td>es</td>
    </tr>
    <tr>
      <th>17</th>
      <td>JOB6e257188735c11befb96c09fedda58ee</td>
      <td>['bbrso181270:review and update of national dr...</td>
      <td>bbrso181270 : review and update of national dr...</td>
      <td>BB</td>
      <td>Latin America and the Carribean</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>International Consultant</td>
      <td>English</td>
      <td>International Consultant</td>
      <td>to apply, interested persons should upload the...</td>
      <td>1.1review and analysis of the draft gender pol...</td>
      <td>sound understanding of national and local deve...</td>
      <td>master s in development studies, socio-economi...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>18</th>
      <td>JOBd62c41b2f523acf8e5bf3411a28d6067</td>
      <td>['long-term agreement - national senior govern...</td>
      <td>long-term agreement - national senior governan...</td>
      <td>LB</td>
      <td>Western Asia</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>National Consultant</td>
      <td>English, Arabic</td>
      <td>National Consultant</td>
      <td>undp launched the anti-corruption for trust in...</td>
      <td>under the supervision of act project manager, ...</td>
      <td>functional competencies : cultural, gender, re...</td>
      <td>i. academic qualifications : a minimum of a ba...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>19</th>
      <td>JOB5851042b6c81564dc853bd062a4f4ecd</td>
      <td>['national consultant-conduct capacity needs a...</td>
      <td>national consultant-conduct capacity needs ass...</td>
      <td>ET</td>
      <td>Sub-Saharian Africa</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>National Consultant</td>
      <td>English</td>
      <td>National Consultant</td>
      <td>ethiopia has adopted normative frameworks on g...</td>
      <td>the national consultant will be responsible to...</td>
      <td>core values and guiding principles demonstrate...</td>
      <td>education advanced university degree (master's...</td>
      <td>en</td>
    </tr>
    <tr>
      <th>21</th>
      <td>JOBbebd1008e8e31383d3a5d26e2085f293</td>
      <td>['international consultant legal analysis for ...</td>
      <td>international consultant legal analysis for in...</td>
      <td>LA</td>
      <td>South-Eastern Asia</td>
      <td>2022</td>
      <td>Individual Contract</td>
      <td>International Consultant</td>
      <td>Unspecified</td>
      <td>International Consultant</td>
      <td>unc df is the united nations capital investmen...</td>
      <td>the legal review documentation and associated ...</td>
      <td>owns a comprehensive knowledge of the organic ...</td>
      <td>minimum master s degree in finance related are...</td>
      <td>en</td>
    </tr>
  </tbody>
</table>
</div>




```python

```
