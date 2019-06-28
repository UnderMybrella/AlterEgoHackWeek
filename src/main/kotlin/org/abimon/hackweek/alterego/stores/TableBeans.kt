package org.abimon.hackweek.alterego.stores

import discord4j.core.`object`.data.stored.AttachmentBean
import discord4j.core.`object`.data.stored.MessageBean
import discord4j.core.`object`.data.stored.embed.*
import java.sql.ResultSet

@ExperimentalUnsignedTypes
data class MessageTableBean(
    val id: String,
    val channelId: String,
    val authorId: String?,
    val content: String?,
    val timestamp: String,
    val editedTimestamp: String?,
    val tts: Boolean,
    val mentionsEveryone: Boolean,
    val pinned: Boolean,
    val webhookId: String?,
    val type: Int
) {
    constructor(rs: ResultSet) : this(
        rs.getString("id"),
        rs.getString("channel_id"),
        rs.getString("author_id"),
        rs.getString("content"),
        rs.getString("timestamp"),
        rs.getString("edited_timestamp"),
        rs.getBoolean("tts"),
        rs.getBoolean("mentions_everyone"),
        rs.getBoolean("pinned"),
        rs.getString("webhook_id"),
        rs.getInt("type")
    )

    fun toBean(): MessageBean {
        val bean = MessageBean()

        bean.id = id.toULong().toLong()
        bean.channelId = channelId.toULong().toLong()
        bean.content = content ?: ""
        bean.timestamp = timestamp
        bean.editedTimestamp = editedTimestamp
        bean.isTts = tts
        bean.isMentionEveryone = mentionsEveryone
        bean.isPinned = pinned
        bean.webhookId = webhookId?.toULong()?.toLong()
        bean.type = type

        return bean
    }
}

data class AttachmentTableBean(
    val id: String,
    val fileName: String,
    val size: Int,
    val url: String,
    val proxyUrl: String,
    val height: Int?,
    val width: Int?
) {
    constructor(rs: ResultSet) : this(
        rs.getString("id"),
        rs.getString("file_name"),
        rs.getInt("size"),
        rs.getString("url"),
        rs.getString("proxy_url"),
        rs.getInt("height"),
        rs.getInt("width")
    )

    fun toAttachmentBean(): AttachmentBean {
        val bean = AttachmentBean()
        bean.fileName = fileName
        bean.size = size
        bean.url = url
        bean.proxyUrl = proxyUrl
        bean.height = height
        bean.width = width

        return bean
    }
}

data class EmbedTableBean(
    val id: String,
    val title: String?,
    val type: String,
    val description: String?,
    val url: String?,
    val timestamp: String?,
    val color: Int?,
    val footerText: String?,
    val footerIconUrl: String?,
    val footerProxyIconUrl: String?,
    val imageUrl: String?,
    val imageProxyUrl: String?,
    val imageHeight: Int?,
    val imageWidth: Int?,
    val thumbnailUrl: String?,
    val thumbnailProxyUrl: String?,
    val thumbnailHeight: Int?,
    val thumbnailWidth: Int?,
    val videoUrl: String?,
    val videoProxyUrl: String?,
    val videoHeight: Int?,
    val videoWidth: Int?,
    val providerName: String?,
    val providerUrl: String?,
    val authorName: String?,
    val authorUrl: String?,
    val authorIconUrl: String?,
    val authorProxyIconUrl: String?
) {
    constructor(rs: ResultSet) : this(
        rs.getString("id"),
        rs.getString("title"),
        rs.getString("type"),
        rs.getString("description"),
        rs.getString("url"),
        rs.getString("timestamp"),
        rs.getInt("color"),
        rs.getString("footer_text"),
        rs.getString("footer_icon_url"),
        rs.getString("footer_proxy_icon_url"),
        rs.getString("image_url"),
        rs.getString("image_proxy_url"),
        rs.getInt("image_height"),
        rs.getInt("image_width"),
        rs.getString("thumbnail_url"),
        rs.getString("thumbnail_proxy_url"),
        rs.getInt("thumbnail_height"),
        rs.getInt("thumbnail_width"),
        rs.getString("video_url"),
        rs.getString("video_proxy_url"),
        rs.getInt("video_height"),
        rs.getInt("video_width"),
        rs.getString("provider_name"),
        rs.getString("provider_url"),
        rs.getString("author_name"),
        rs.getString("author_url"),
        rs.getString("author_icon_url"),
        rs.getString("author_proxy_icon_url")
    )

    val footerBean: EmbedFooterBean? by lazy {
        if (footerText != null) {
            val bean = EmbedFooterBean()
            bean.text = footerText

            footerIconUrl?.apply { bean.iconUrl = this }
            footerProxyIconUrl?.apply { bean.proxyIconUrl = this }

            bean
        } else {
            null
        }
    }
    val imageBean: EmbedImageBean? by lazy {
        if (imageUrl != null) {
            val bean = EmbedImageBean()
            bean.url = imageUrl
            imageProxyUrl?.apply { bean.proxyUrl = this }
            imageHeight?.apply { bean.height = this }
            imageWidth?.apply { bean.width = this }

            bean
        } else {
            null
        }
    }
    val thumbnailBean: EmbedThumbnailBean? by lazy {
        if (thumbnailUrl != null) {
            val bean = EmbedThumbnailBean()
            bean.url = thumbnailUrl
            thumbnailProxyUrl?.apply { bean.proxyUrl = this }
            thumbnailHeight?.apply { bean.height = this }
            thumbnailWidth?.apply { bean.width = this }

            bean
        } else {
            null
        }
    }
    val videoBean: EmbedVideoBean? by lazy {
        if (videoUrl != null) {
            val bean = EmbedVideoBean()
            bean.url = videoUrl
            videoProxyUrl?.apply { bean.proxyUrl = this }
            videoHeight?.apply { bean.height = this }
            videoWidth?.apply { bean.width = this }

            bean
        } else {
            null
        }
    }
    val providerBean: EmbedProviderBean? by lazy {
        if (providerName != null || providerUrl != null) {
            val bean = EmbedProviderBean()
            providerName?.apply { bean.name = this }
            providerUrl?.apply { bean.url = this }

            bean
        } else {
            null
        }
    }
    val authorBean: EmbedAuthorBean? by lazy {
        if (authorName != null) {
            val bean = EmbedAuthorBean()
            bean.name = authorName

            authorUrl?.apply { bean.url = this }
            authorIconUrl?.apply { bean.iconUrl = this }
            authorProxyIconUrl?.apply { bean.proxyIconUrl = this }

            bean
        } else {
            null
        }
    }

    fun toBean(): EmbedBean {
        val bean = EmbedBean()

        bean.title = title
        bean.type = type
        bean.description = description
        bean.url = url
        bean.timestamp = timestamp
        bean.color = color

        bean.footer = footerBean
        bean.image = imageBean
        bean.thumbnail = thumbnailBean
        bean.video = videoBean
        bean.provider = providerBean
        bean.author = authorBean

        return bean
    }
}