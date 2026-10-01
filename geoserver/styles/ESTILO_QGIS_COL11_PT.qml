<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis maxScale="0" styleCategories="AllStyleCategories" hasScaleBasedVisibilityFlag="0" minScale="1e+08" version="3.28.3-Firenze">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>1</Searchable>
    <Private>0</Private>
  </flags>
  <customproperties>
    <Option type="Map">
      <Option value="false" type="bool" name="WMSBackgroundLayer"/>
      <Option value="false" type="bool" name="WMSPublishDataSourceUrl"/>
      <Option value="0" type="int" name="embeddedWidgets/count"/>
      <Option value="Value" type="QString" name="identify/format"/>
    </Option>
  </customproperties>
  <pipe-data-defined-properties>
    <Option type="Map">
      <Option value="" type="QString" name="name"/>
      <Option name="properties"/>
      <Option value="collection" type="QString" name="type"/>
    </Option>
  </pipe-data-defined-properties>
  <pipe>
    <provider>
      <resampling enabled="false" zoomedInResamplingMethod="nearestNeighbour" zoomedOutResamplingMethod="nearestNeighbour" maxOversampling="2"/>
    </provider>
    <rasterrenderer opacity="1" alphaBand="-1" band="1" type="paletted" nodataColor="">
      <rasterTransparency/>
      <minMaxOrigin>
        <limits>None</limits>
        <extent>WholeRaster</extent>
        <statAccuracy>Estimated</statAccuracy>
        <cumulativeCutLower>0.02</cumulativeCutLower>
        <cumulativeCutUpper>0.98</cumulativeCutUpper>
        <stdDevFactor>2</stdDevFactor>
      </minMaxOrigin>
      <colorPalette>
        <paletteEntry value="3" color="#1f8d49" alpha="255" label="Formação Florestal"/>
        <paletteEntry value="4" color="#7dc975" alpha="255" label="Formação Savânica"/>
        <paletteEntry value="5" color="#04381d" alpha="255" label="Mangue"/>
        <paletteEntry value="6" color="#007785" alpha="255" label="Floresta Alagável"/>
        <paletteEntry value="7" color="#228c70" alpha="255" label="Savana Alagada (beta)"/>
        <paletteEntry value="9" color="#7a5900" alpha="255" label="Silvicultura"/>
        <paletteEntry value="11" color="#519799" alpha="255" label="Campo Alagado e Área Pantanosa"/>
        <paletteEntry value="12" color="#d6bc74" alpha="255" label="Formação Campestre"/>
        <paletteEntry value="15" color="#edde8e" alpha="255" label="Pastagem"/>
        <paletteEntry value="20" color="#db7093" alpha="255" label="Cana"/>
        <paletteEntry value="21" color="#ffefc3" alpha="255" label="Mosaico de Usos"/>
        <paletteEntry value="23" color="#ffa07a" alpha="255" label="Praia, Duna e Areal"/>
        <paletteEntry value="24" color="#d4271e" alpha="255" label="Área Urbanizada"/>
        <paletteEntry value="25" color="#db4d4f" alpha="255" label="Outras Áreas não Vegetadas"/>
        <paletteEntry value="29" color="#ad5100" alpha="255" label="Afloramento Rochoso"/>
        <paletteEntry value="30" color="#9c0027" alpha="255" label="Mineração"/>
        <paletteEntry value="31" color="#091077" alpha="255" label="Aquicultura"/>
        <paletteEntry value="32" color="#fc8114" alpha="255" label="Apicum"/>
        <paletteEntry value="33" color="#2532e4" alpha="255" label="Rio, Lago e Oceano"/>
        <paletteEntry value="35" color="#9065d0" alpha="255" label="Dendê"/>
        <paletteEntry value="39" color="#f5b3c8" alpha="255" label="Soja"/>
        <paletteEntry value="40" color="#c71585" alpha="255" label="Arroz"/>
        <paletteEntry value="41" color="#f54ca9" alpha="255" label="Outras Lavouras Temporárias"/>
        <paletteEntry value="46" color="#d68fe2" alpha="255" label="Café"/>
        <paletteEntry value="47" color="#9932cc" alpha="255" label="Citrus"/>
        <paletteEntry value="48" color="#e6ccff" alpha="255" label="Outras Lavouras Perenes"/>
        <paletteEntry value="49" color="#02d659" alpha="255" label="Restinga Arbórea"/>
        <paletteEntry value="50" color="#ffaa5f" alpha="255" label="Restinga Herbácea ou Arbustiva"/>
        <paletteEntry value="62" color="#ff69b4" alpha="255" label="Algodão (beta)"/>
        <paletteEntry value="75" color="#757272" alpha="255" label="Usina Fotovoltaica"/>
        <paletteEntry value="77" color="#86b074" alpha="255" label="Formação Herbáceo Arbustiva"/>
        <paletteEntry value="84" color="#81dbbf" alpha="255" label="Marismas (beta)"/>
        <paletteEntry value="91" color="#403d3e" alpha="255" label="Parque Eólico (beta)"/>
      </colorPalette>
      <colorramp type="randomcolors" name="[source]">
        <Option/>
      </colorramp>
    </rasterrenderer>
    <brightnesscontrast gamma="1" brightness="0" contrast="0"/>
    <huesaturation colorizeOn="0" colorizeGreen="128" grayscaleMode="0" colorizeRed="255" colorizeStrength="100" invertColors="0" colorizeBlue="128" saturation="0"/>
    <rasterresampler maxOversampling="2"/>
    <resamplingStage>resamplingFilter</resamplingStage>
  </pipe>
  <blendMode>0</blendMode>
</qgis>
